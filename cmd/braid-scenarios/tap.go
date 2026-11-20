package main

import (
	"context"
	"io"
	"sync"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	kolotransport "github.com/ciranda-dev/kolo-transport"

	"github.com/ciranda-dev/kolo-playground/internal/braidwire"
)

// Tap is a pin that every peer links to: it counts the braid messages it receives by type. Peers send it the same
// announcements as any member, so this is what one link costs.
type Tap struct {
	id     *koloid.Identity
	l      *kolosecure.Listener
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu    sync.Mutex
	msgs  map[byte]int
	bytes map[byte]int
	links int
	bad   int
}

func newTap(ctx context.Context) (*Tap, error) {
	id, err := koloid.Generate()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	l, err := kolosecure.Listen(ctx, "127.0.0.1:0", id, &kolosecure.Config{Transport: &kolotransport.Config{MaxConnsPerIP: 64, MaxConns: 256}})
	if err != nil {
		cancel()
		return nil, err
	}
	t := &Tap{id: id, l: l, cancel: cancel, msgs: map[byte]int{}, bytes: map[byte]int{}}
	t.wg.Go(func() {
		for {
			c, err := l.Accept(ctx)
			if err != nil {
				return
			}
			t.mu.Lock()
			t.links++
			t.mu.Unlock()
			t.wg.Go(func() { t.serve(ctx, c) })
		}
	})
	return t, nil
}

// Target is the ADDR@KEY a peer links to.
func (t *Tap) Target() string { return t.l.Addr().String() + "@" + hexKey(t.id.PublicKey()) }

func (t *Tap) serve(ctx context.Context, c *kolosecure.Conn) {
	defer c.Close()
	t.wg.Go(func() {
		for {
			st, err := c.AcceptStream(ctx)
			if err != nil {
				return
			}
			st.CancelRead(0)
			st.CancelWrite(0)
		}
	})
	for {
		st, err := c.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		frame, err := io.ReadAll(io.LimitReader(st, braidwire.MaxFrame+8))
		st.CancelRead(0)
		ty, ok := braidwire.Type(frame)
		t.mu.Lock()
		if err != nil || !ok {
			t.bad++
		} else {
			t.msgs[ty]++
			t.bytes[ty] += len(frame)
		}
		t.mu.Unlock()
		if ok {
			t.answer(ctx, c, ty)
		}
	}
}

// answer replies that the tap holds nothing, so that a peer's request does not wait for its timeout.
func (t *Tap) answer(ctx context.Context, c *kolosecure.Conn, ty byte) {
	var frame []byte
	switch ty {
	case braidwire.RecordReq:
		frame = braidwire.EmptyRecord()
	case braidwire.EntryReq:
		frame = braidwire.EmptyEntry()
	default:
		return
	}
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	st, err := c.OpenUniStream(sctx)
	if err != nil {
		return
	}
	st.Write(frame)
	st.Close()
}

// TapStats are the totals the tap counted.
type TapStats struct {
	AnnounceMsgs, AnnounceBytes int
	EvidenceMsgs, EvidenceBytes int
	RequestMsgs, RequestBytes   int
	AnswerMsgs, AnswerBytes     int
	Links, Undecodable          int
}

// Stats returns what the tap has counted so far.
func (t *Tap) Stats() TapStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := TapStats{Links: t.links, Undecodable: t.bad,
		AnnounceMsgs: t.msgs[braidwire.Announce], AnnounceBytes: t.bytes[braidwire.Announce],
		EvidenceMsgs: t.msgs[braidwire.Evidence], EvidenceBytes: t.bytes[braidwire.Evidence]}
	for _, ty := range []byte{braidwire.RecordReq, braidwire.EntryReq} {
		s.RequestMsgs += t.msgs[ty]
		s.RequestBytes += t.bytes[ty]
	}
	for _, ty := range []byte{braidwire.Record, braidwire.Entry} {
		s.AnswerMsgs += t.msgs[ty]
		s.AnswerBytes += t.bytes[ty]
	}
	return s
}

// Close stops the tap and waits for its goroutines.
func (t *Tap) Close() {
	t.cancel()
	t.l.Close()
	t.wg.Wait()
}
