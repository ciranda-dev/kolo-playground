// Package braidwire builds and reads the frames of the kolo-braid stream protocol that the playground's measuring pin and
// hostile peer need: a little-endian uint32 length, then uvarint version 0, uvarint type and the fields.
package braidwire

import (
	"encoding/binary"

	kololog "github.com/ciranda-dev/kolo-log"
)

// Message types of the stream protocol.
const (
	Announce  byte = 1
	Evidence  byte = 2
	RecordReq byte = 3
	Record    byte = 4
	EntryReq  byte = 5
	Entry     byte = 6
)

// MaxFrame is the longest frame, length prefix excluded.
const MaxFrame = 4 << 20

func frame(t byte, fields []byte) []byte {
	p := append([]byte{0, t}, fields...)
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(p))), p...)
}

// Type returns the type of a whole frame, false when its length or version is wrong or its type unknown.
func Type(f []byte) (byte, bool) {
	if len(f) < 6 || binary.LittleEndian.Uint32(f) != uint32(len(f)-4) || f[4] != 0 || f[5] < Announce || f[5] > Entry {
		return 0, false
	}
	return f[5], true
}

// EmptyRecord is the RECORD answer of a peer that holds nothing.
func EmptyRecord() []byte { return frame(Record, []byte{0}) }

// EmptyEntry is the ENTRY answer of a peer that holds nothing.
func EmptyEntry() []byte { return frame(Entry, []byte{0, 0}) }

// AnnounceFrame is an ANNOUNCE of heads for a group.
func AnnounceFrame(group [32]byte, heads []kololog.Head) []byte {
	f := append([]byte{}, group[:]...)
	f = binary.AppendUvarint(f, uint64(len(heads)))
	for _, h := range heads {
		enc := h.Encode()
		f = binary.AppendUvarint(f, uint64(len(enc)))
		f = append(f, enc...)
	}
	return frame(Announce, f)
}
