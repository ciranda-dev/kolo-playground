package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	koloswarm "github.com/ciranda-dev/kolo-swarm"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

func TestPing(t *testing.T) {
	checkGoroutines(t)
	server := newServer(t)
	a, b := newSwarm(t, server, nil), newSwarm(t, server, nil)
	pa, pb := connected(t, a, b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	wg.Add(1)
	go func() {
		defer wg.Done()
		respond(ctx, pb.Conn(), 300*time.Millisecond)
	}()
	for range 3 {
		rtt, err := ping(ctx, pa.Conn(), 2*time.Second)
		if err != nil || rtt <= 0 {
			t.Fatalf("ping: %v, rtt %v", err, rtt)
		}
	}

	send := func(msg []byte, finish bool) error {
		st, err := pa.Conn().OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		st.SetDeadline(time.Now().Add(3 * time.Second))
		go func() {
			st.Write(msg)
			if finish {
				st.Close()
			}
		}()
		reply, err := io.ReadAll(st)
		if err == nil && len(reply) > 0 {
			t.Errorf("%d-byte request: got a %d-byte reply", len(msg), len(reply))
		}
		return err
	}
	good := make([]byte, pingSize)
	for name, msg := range map[string][]byte{
		"empty":     {},
		"short":     good[:8],
		"long":      append(good, 0),
		"version 1": append([]byte{1}, good[1:]...),
		"1 MiB":     make([]byte, 1<<20),
	} {
		if err := send(msg, true); !errors.Is(err, kolotransport.ErrStreamReset) {
			t.Errorf("%s: %v, want ErrStreamReset", name, err)
		}
	}
	if err := send(good, false); !errors.Is(err, kolotransport.ErrStreamReset) {
		t.Errorf("unfinished request: %v, want ErrStreamReset", err)
	}
	if _, err := ping(ctx, pa.Conn(), 2*time.Second); err != nil {
		t.Errorf("ping after bad requests: %v", err)
	}
}

func TestPingBadReply(t *testing.T) {
	checkGoroutines(t)
	server := newServer(t)
	a, b := newSwarm(t, server, nil), newSwarm(t, server, nil)
	pa, pb := connected(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		st, err := pb.Conn().AcceptStream(ctx)
		if err != nil {
			return
		}
		req, _ := io.ReadAll(io.LimitReader(st, pingSize+1))
		if len(req) > 0 {
			req[len(req)-1] ^= 1
		}
		st.Write(req)
		st.Close()
	}()
	if _, err := ping(ctx, pa.Conn(), 2*time.Second); err == nil || err.Error() != "bad reply" {
		t.Errorf("ping with a wrong echo: %v, want bad reply", err)
	}
}

// TestRespondOneAtATime checks that a ping waits while the responder holds an unfinished stream.
func TestRespondOneAtATime(t *testing.T) {
	checkGoroutines(t)
	server := newServer(t)
	a, b := newSwarm(t, server, nil), newSwarm(t, server, nil)
	pa, pb := connected(t, a, b)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	wg.Add(1)
	go func() {
		defer wg.Done()
		respond(ctx, pb.Conn(), time.Second)
	}()
	held, err := pa.Conn().OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.CancelRead(0)
	if _, err := held.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := ping(ctx, pa.Conn(), 3*time.Second); err != nil || time.Since(start) < 900*time.Millisecond {
		t.Errorf("ping: %v after %v, want an echo after the held stream's 1s timeout", err, time.Since(start))
	}
}

func TestPingTimeout(t *testing.T) {
	checkGoroutines(t)
	server := newServer(t)
	a, b := newSwarm(t, server, nil), newSwarm(t, server, nil)
	pa, _ := connected(t, a, b)
	start := time.Now()
	_, err := ping(context.Background(), pa.Conn(), 300*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Errorf("ping without a responder: %v after %v", err, time.Since(start))
	}
}

// TestAllowRefusesLoopbackPeers checks that a swarm with --allow-loopback off refuses a peer that dials it over loopback.
func TestAllowRefusesLoopbackPeers(t *testing.T) {
	checkGoroutines(t)
	server := newServer(t)
	var mu sync.Mutex
	var refused []netip.AddrPort
	strict := allow(false)
	a := newSwarm(t, server, &koloswarm.Config{Allow: func(k ed25519.PublicKey, ap netip.AddrPort) bool {
		ok := strict(k, ap)
		if !ok {
			mu.Lock()
			refused = append(refused, ap)
			mu.Unlock()
		}
		return ok
	}})
	b := newSwarm(t, server, &koloswarm.Config{Allow: allow(true)})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	topic := topicOf("test")
	for _, s := range []*koloswarm.Swarm{a, b} {
		if err := s.Join(ctx, topic, nil); err != nil {
			t.Fatal(err)
		}
	}
	var first netip.AddrPort
	for !first.IsValid() {
		mu.Lock()
		if len(refused) > 0 {
			first = refused[0]
		}
		mu.Unlock()
		if ctx.Err() != nil {
			t.Fatal("a never refused a loopback connection")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !first.Addr().IsLoopback() {
		t.Errorf("refused %s", first)
	}
	wctx, wcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer wcancel()
	for _, s := range []*koloswarm.Swarm{a, b} {
		for {
			ev, err := s.Next(wctx)
			if err != nil {
				break
			}
			if ev.Kind == koloswarm.Connected {
				t.Errorf("connected to %x despite the refusal", ev.Peer.PublicKey()[:4])
			}
		}
	}
}

func FuzzPing(f *testing.F) {
	f.Add(make([]byte, pingSize))
	f.Add([]byte{0x80, 0, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, b []byte) {
		ok := decodePing(b)
		if want := len(b) == pingSize && b[0] == 0; ok != want {
			t.Fatalf("decodePing(%x) = %v", b, ok)
		}
		if ok && !bytes.Equal(append([]byte{0}, b[1:]...), b) {
			t.Fatal("accepted message does not re-encode")
		}
	})
}
