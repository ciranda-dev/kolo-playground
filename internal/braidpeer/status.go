package braidpeer

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"

	kolobraid "github.com/ciranda-dev/kolo-braid"
)

// counterKey holds the number of effective data entries applied, so peers compare how much each has settled.
var counterKey = []byte("#")

// Status is what the status command prints: one JSON object.
type Status struct {
	Name string `json:"name"`
	Key  string `json:"key"`
	Addr string `json:"addr"`

	HasBraid bool   `json:"hasBraid"`
	Strand   string `json:"strand,omitempty"`
	Group    string `json:"group,omitempty"`

	Settled       uint64 `json:"settled"`
	Tentative     uint64 `json:"tentative"`
	Tail          int    `json:"tail"`
	SettledData   uint64 `json:"settledData"`
	TentativeData uint64 `json:"tentativeData"`
	Content       string `json:"content"`
	Order         string `json:"order"`
	OrderAt       uint64 `json:"orderAt"`

	Config           uint64         `json:"config"`
	Seats            int            `json:"seats"`
	Quorum           int            `json:"quorum"`
	Pending          []string       `json:"pending,omitempty"`
	Awaiting         []string       `json:"awaiting,omitempty"`
	Members          []string       `json:"members"`
	SettledMembers   []string       `json:"settledMembers"`
	SeatKeys         []string       `json:"seatKeys"`
	Unconfirmed      bool           `json:"unconfirmed"`
	StrandLost       bool           `json:"strandLost"`
	SettlementHalted string         `json:"settlementHalted,omitempty"`
	Halted           string         `json:"halted,omitempty"`
	EventsDropped    uint64         `json:"eventsDropped"`
	Events           map[string]int `json:"events"`
	Links            int            `json:"links"`
	Commits          int64          `json:"commits"`
	ReplicaCommits   int64          `json:"replicaCommits"`
	ReplicaBytes     int64          `json:"replicaBytes"`
	OwnCommits       int64          `json:"ownCommits"`
	UDPInBytes       int64          `json:"udpInBytes"`
	UDPOutBytes      int64          `json:"udpOutBytes"`
	UDPIn            int64          `json:"udpIn"`
	UDPOut           int64          `json:"udpOut"`
	AdmitNanos       int64          `json:"admitNanos,omitempty"`
}

// orderCache is the order digest of the settled prefix up to pos; the settled prefix never changes, so it only grows.
type orderCache struct {
	pos uint64
	sum [32]byte
}

// orderStep extends the digest with one ordered entry.
func orderStep(sum [32]byte, o kolobraid.Ordered) [32]byte {
	h := sha256.New()
	h.Write(sum[:])
	h.Write(o.ID[:])
	var f byte
	if o.Void {
		f |= 1
	}
	if o.Rejected {
		f |= 2
	}
	h.Write([]byte{f})
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// orderTo returns the digest of the first n settled positions, extending from base.
func orderTo(b *kolobraid.Braid, base orderCache, n uint64) (orderCache, error) {
	if n < base.pos {
		base = orderCache{}
	}
	pos, sum := base.pos, base.sum
	if pos < n {
		for o, err := range b.Entries(pos) {
			if err != nil {
				return base, err
			}
			if !o.Settled || pos >= n {
				break
			}
			sum = orderStep(sum, o)
			pos++
		}
	}
	if pos != n {
		return base, errors.New("braidpeer: settled prefix shorter than asked")
	}
	return orderCache{pos: pos, sum: sum}, nil
}

func dataCount(s *kolobraid.Snapshot) uint64 {
	v, err := s.Get(counterKey)
	if err != nil || len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func hexOf(b []byte) string { return hex.EncodeToString(b) }
