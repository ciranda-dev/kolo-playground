package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/netip"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	kolodht "github.com/ciranda-dev/kolo-dht"
	koloid "github.com/ciranda-dev/kolo-id"
	kolorelay "github.com/ciranda-dev/kolo-relay"
	kolosecure "github.com/ciranda-dev/kolo-secure"
	koloswarm "github.com/ciranda-dev/kolo-swarm"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

// relayServer starts a kolo-relay server on loopback and returns its --relay value. Every client comes from
// 127.0.0.1, so the per-address connection limit is raised.
func relayServer(t *testing.T, cfg *kolorelay.ServerConfig) string {
	t.Helper()
	if cfg == nil {
		cfg = &kolorelay.ServerConfig{}
	}
	cfg.Secure = &kolosecure.Config{Transport: &kolotransport.Config{MaxConnsPerIP: 64, MaxDialed: 1}}
	id := ident(t)
	r, err := kolorelay.Listen(context.Background(), "127.0.0.1:0", id, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return fmt.Sprintf("%s@%x", r.Addr(), []byte(id.PublicKey()))
}

// heldRelay starts a relay that holds every Connect for hold; the func reports the most it held at once per initiator
// and per key pair.
func heldRelay(t *testing.T, hold time.Duration) (string, func() (initiator, pair int)) {
	t.Helper()
	var mu sync.Mutex
	held := map[string]int{}
	var most, mostPair int
	relay := relayServer(t, &kolorelay.ServerConfig{AllowConnect: func(from, to ed25519.PublicKey) bool {
		f, k := string(from), string(from)+string(to)
		mu.Lock()
		held[f]++
		held[k]++
		most, mostPair = max(most, held[f]), max(mostPair, held[k])
		mu.Unlock()
		time.Sleep(hold)
		mu.Lock()
		held[f]--
		held[k]--
		mu.Unlock()
		return true
	}})
	return relay, func() (int, int) {
		mu.Lock()
		defer mu.Unlock()
		return most, mostPair
	}
}

// firewall makes a swarm's QUIC listener refuse every connection, so a dial fails after the punch; kolo-dht still works.
func firewall() *koloswarm.Config {
	return &koloswarm.Config{DHT: &kolodht.Config{Secure: &kolosecure.Config{Transport: &kolotransport.Config{
		AcceptFunc: func(netip.AddrPort) bool { return false },
	}}}}
}

type testPeer struct {
	out  *lines
	id   string
	s    *koloswarm.Swarm
	me   *koloid.Identity
	stop func()
}

// servePeer runs serve on a new loopback swarm with cfg, and shuts it down as run does at cleanup.
func servePeer(t *testing.T, cfg *koloswarm.Config, args ...string) *testPeer {
	t.Helper()
	o, err := parseFlags(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		cfg = &koloswarm.Config{}
	}
	cfg.MaxPeers = o.maxPeers
	p := &testPeer{out: &lines{}, me: ident(t)}
	p.id = short(p.me.PublicKey())
	if p.s, err = koloswarm.New(context.Background(), "127.0.0.1:0", p.me, cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		serve(ctx, p.s, p.me, o, log.New(p.out, "", log.Ltime), &wg)
		close(done)
	}()
	p.stop = sync.OnceFunc(func() {
		cancel()
		<-done
		p.s.Close()
		wg.Wait()
	})
	t.Cleanup(p.stop)
	return p
}

// reserve reserves p's key on relay and attaches the peers that reach it there, as --relay does.
func reserve(t *testing.T, p *testPeer, relay string) {
	t.Helper()
	addr, key, _ := strings.Cut(relay, "@")
	k, _ := hex.DecodeString(key)
	ctx, cancel := context.WithCancel(context.Background())
	res, err := kolorelay.Reserve(ctx, addr, k, p.me, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		accept(ctx, p.s, res, log.New(io.Discard, "", 0), &wg)
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

// TestRelayed runs two peers whose direct path fails: they connect through a relay and ping, also after it ends
// their reservations.
func TestRelayed(t *testing.T) {
	checkGoroutines(t)
	relay := relayServer(t, &kolorelay.ServerConfig{ReservationLifetime: 5 * time.Second})
	args := []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--relay", relay, "--allow-loopback"}
	b := servePeer(t, firewall(), args...)
	b.out.wait(t, line(fmtReserved, "", ""), 10*time.Second)
	b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	a := servePeer(t, nil, args...)

	a.out.wait(t, line(fmtFound, b.id, ""), 20*time.Second)
	a.out.wait(t, line(fmtUnreachR, b.id, ""), 30*time.Second)
	a.out.wait(t, line(fmtConnR, b.id, ""), 20*time.Second)
	b.out.wait(t, line(fmtConnR, a.id, ""), 20*time.Second)
	a.out.wait(t, line(fmtPing, b.id, ""), 20*time.Second)
	b.out.wait(t, line(fmtPing, a.id, ""), 20*time.Second)
	if a.out.has(line(fmtConn, b.id, "")) || b.out.has(line(fmtConn, a.id, "")) {
		t.Error("connected directly through the firewall")
	}
	if a.out.has(line(fmtUnreach, b.id, "")) {
		t.Error("the no relay line with --relay")
	}

	for _, p := range []*testPeer{a, b} {
		p.out.wait(t, line(fmtEnded, "", ""), 10*time.Second)
	}
	na, nb := len(a.out.all()), len(b.out.all())
	a.out.waitFrom(t, na, line(fmtPing, b.id, ""), 20*time.Second)
	b.out.waitFrom(t, nb, line(fmtPing, a.id, ""), 20*time.Second)
	if a.out.has(line(tm+`peer lost`, "", "")) || b.out.has(line(tm+`peer lost`, "", "")) {
		t.Error("a relayed peer was lost with the reservation")
	}
}

// TestRelayRetry checks that a relay dial that failed, since the peer had not reserved yet, is tried again.
func TestRelayRetry(t *testing.T) {
	checkGoroutines(t)
	relay := relayServer(t, nil)
	server := newServer(t).String()
	b := servePeer(t, firewall(), "--bootstrap", server, "--topic", "test", "--allow-loopback")
	b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	a := servePeer(t, nil, "--bootstrap", server, "--topic", "test", "--relay", relay, "--allow-loopback")
	a.out.wait(t, line(fmtRelayFailed, b.id, ""), 30*time.Second)
	reserve(t, b, relay)
	a.out.wait(t, line(fmtConnR, b.id, ""), 60*time.Second)
	b.out.wait(t, line(fmtConnR, a.id, ""), 10*time.Second)
}

// TestRelayDedupe checks that unreachable events that repeat during a relay dial start no other dial to that peer.
func TestRelayDedupe(t *testing.T) {
	checkGoroutines(t)
	relay, most := heldRelay(t, 4*time.Second)
	args := []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--relay", relay, "--allow-loopback"}
	b := servePeer(t, firewall(), args...)
	b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	a := servePeer(t, nil, args...)
	a.out.wait(t, line(fmtConnR, b.id, ""), 40*time.Second)
	if _, pair := most(); pair != 1 {
		t.Errorf("%d relay dials to one peer at once, want 1", pair)
	}
}

// TestRelayBound checks that relay dials count as outbound: a, with --max-peers 2 and so 1 inbound, relays to 2 of 3 peers.
func TestRelayBound(t *testing.T) {
	checkGoroutines(t)
	relay, most := heldRelay(t, 3*time.Second)
	args := []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--relay", relay, "--allow-loopback"}
	for range 3 {
		servePeer(t, firewall(), args...).out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	}
	a := servePeer(t, nil, append(args, "--max-peers", "2")...)
	relayed := line(fmtConnR, shortID, "")
	for deadline := time.Now().Add(40 * time.Second); a.out.count(relayed) < 2; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("not 2 relayed peers; got:\n%s", strings.Join(a.out.all(), "\n"))
		}
	}
	if n, _ := most(); n > 2 {
		t.Errorf("%d relay dials from one peer at once, want at most 2", n)
	}
}

// TestRelaySlotFreed checks that a dial while the only slot is taken is logged, and that a finished dial frees the slot.
func TestRelaySlotFreed(t *testing.T) {
	checkGoroutines(t)
	relay, _ := heldRelay(t, 3*time.Second)
	args := []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--relay", relay, "--allow-loopback"}
	bs := map[string]*testPeer{}
	for range 2 {
		b := servePeer(t, firewall(), args...)
		b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
		bs[b.id] = b
	}
	a := servePeer(t, nil, append(args, "--max-peers", "1")...)
	first := a.out.wait(t, line(fmtConnR, shortID, ""), 40*time.Second)[1]
	other := ""
	for id := range bs {
		if id != first {
			other = id
		}
	}
	a.out.wait(t, line(fmtUnreachBusy, other, ""), 0)
	bs[first].stop()
	a.out.wait(t, line(tm+`peer lost ID: `, first, ""), 20*time.Second)
	a.out.wait(t, line(fmtConnR, other, ""), 60*time.Second)
}

// shortDialTimeout sets relayDialTimeout to d until the test's later cleanups, the peers' included, have run.
func shortDialTimeout(t *testing.T, d time.Duration) {
	old := relayDialTimeout
	t.Cleanup(func() { relayDialTimeout = old })
	relayDialTimeout = d
}

// TestRelayDialTimeout checks that a relay dial held by the relay ends at relayDialTimeout.
func TestRelayDialTimeout(t *testing.T) {
	checkGoroutines(t)
	shortDialTimeout(t, time.Second)
	relay, _ := heldRelay(t, 4*time.Second)
	args := []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--relay", relay, "--allow-loopback"}
	b := servePeer(t, firewall(), args...)
	b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	a := servePeer(t, nil, args...)
	a.out.wait(t, line(fmtUnreachR, b.id, ""), 30*time.Second)
	start := time.Now()
	a.out.wait(t, line(fmtRelayTimeout, b.id, ""), 3*time.Second)
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Errorf("relay dial failed after %v, want about 1s", d)
	}
}

// TestRelayHelloTimeout checks that relayDialTimeout also bounds the hello when b never answers it.
func TestRelayHelloTimeout(t *testing.T) {
	checkGoroutines(t)
	shortDialTimeout(t, time.Second)
	relay := relayServer(t, nil)
	server := newServer(t).String()
	b := servePeer(t, firewall(), "--bootstrap", server, "--topic", "test", "--allow-loopback")
	b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	addr, key, _ := strings.Cut(relay, "@")
	k, _ := hex.DecodeString(key)
	ctx, cancel := context.WithCancel(context.Background())
	res, err := kolorelay.Reserve(ctx, addr, k, b.me, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			if _, err := res.Accept(ctx); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		res.Close()
		<-accepted
	})
	a := servePeer(t, nil, "--bootstrap", server, "--topic", "test", "--relay", relay, "--allow-loopback")
	a.out.wait(t, line(fmtUnreachR, b.id, ""), 30*time.Second)
	start := time.Now()
	a.out.wait(t, line(fmtRelayTimeout, b.id, ""), 3*time.Second)
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Errorf("relay hello failed after %v, want about 1s", d)
	}
}

// TestRelayInbound checks that a connection from the reservation counts as inbound: b refuses a second one.
func TestRelayInbound(t *testing.T) {
	checkGoroutines(t)
	args := []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--relay", relayServer(t, nil), "--allow-loopback"}
	b := servePeer(t, firewall(), append(args, "--max-peers", "2")...)
	b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	a1, a2 := servePeer(t, nil, args...), servePeer(t, nil, args...)
	refused := line(tm+`relay ID failed: koloswarm: connection refused$`, b.id, "")
	for deadline := time.Now().Add(40 * time.Second); !a1.out.has(refused) && !a2.out.has(refused); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("b refused no relayed peer; b:\n%s", strings.Join(b.out.all(), "\n"))
		}
	}
	if n := b.out.count(line(fmtConnR, shortID, "")); n != 1 {
		t.Errorf("b has %d relayed peers, want 1", n)
	}
}

// TestJoinFailsWithRelay checks that serve stops the reservation when it returns early.
func TestJoinFailsWithRelay(t *testing.T) {
	checkGoroutines(t)
	server := newServer(t)
	o, err := parseFlags([]string{"--bootstrap", server.String(), "--topic", "test", "--relay", relayServer(t, nil), "--allow-loopback"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	s := newSwarm(t, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A topic joined twice is ErrInvalidArgument, so serve's Join fails after the reservation.
	if err := s.Join(ctx, topicOf("test"), &koloswarm.JoinOptions{Lookup: true}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	if err := serve(ctx, s, ident(t), o, log.New(io.Discard, "", 0), &wg); err == nil || !strings.HasPrefix(err.Error(), "join: ") {
		t.Fatalf("serve: %v, want a join error", err)
	}
	s.Close()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serve's goroutines still run after it returned")
	}
}

// TestRunRelayed runs run with a relayed peer and checks that nothing it started outlives it.
func TestRunRelayed(t *testing.T) {
	checkGoroutines(t)
	t.Setenv("TMPDIR", t.TempDir())
	args := []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--relay", relayServer(t, nil), "--allow-loopback"}
	b := servePeer(t, firewall(), args...)
	b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	out := &lines{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	code := make(chan int, 1)
	go func() { code <- run(ctx, append(args, "--listen", "127.0.0.1:0"), out, out) }()
	out.wait(t, line(fmtConnR, b.id, ""), 30*time.Second)
	out.wait(t, line(fmtPing, b.id, ""), 20*time.Second)
	runID := goroutineWith("kolo-peer.run(")
	if runID == "" {
		t.Fatal("no goroutine runs run")
	}
	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("exit %d", c)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not return")
	}
	if left := startedBy(runID); len(left) != 0 {
		t.Errorf("%d goroutines that run started still run after it returned:\n%s", len(left), strings.Join(left, "\n\n"))
	}
	out.wait(t, line(fmtStop, "", ""), 0)
	out.wait(t, line(fmtStopped, "", ""), 0)
}

// TestShutdownTimeout checks that a shutdown longer than shutdownTimeout ends with exit 1.
func TestShutdownTimeout(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	defer func(d time.Duration) { shutdownTimeout = d }(shutdownTimeout)
	shutdownTimeout = time.Nanosecond
	out := &lines{}
	var errb bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	code := make(chan int, 1)
	go func() {
		code <- run(ctx, []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--listen", "127.0.0.1:0", "--allow-loopback"}, out, &errb)
	}()
	out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	cancel()
	if c := <-code; c != 1 || errb.String() != "kolo-peer: shutdown timed out\n" {
		t.Errorf("exit %d, stderr %q", c, errb.String())
	}
}

func TestRelayFails(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	addr, _, _ := strings.Cut(relayServer(t, nil), "@")
	wrong := addr + "@" + hex.EncodeToString(ident(t).PublicKey())
	var out, errb bytes.Buffer
	code := run(context.Background(), []string{"--bootstrap", newServer(t).String(), "--topic", "x", "--listen", "127.0.0.1:0",
		"--relay", wrong, "--allow-loopback"}, &out, &errb)
	if code != 1 || errb.String() != "kolo-peer: relay: kolosecure: unexpected peer key\n" {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	if out.Len() == 0 || strings.Contains(out.String(), "relay reserved") {
		t.Errorf("stdout %q", out.String())
	}
}

// TestServeRelay runs kolo-peer as a relay and reserves on it with the key and address it prints.
func TestServeRelay(t *testing.T) {
	checkGoroutines(t)
	t.Setenv("TMPDIR", t.TempDir())
	out := &lines{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	code := make(chan int, 1)
	go func() { code <- run(ctx, []string{"--serve-relay", "--listen", "127.0.0.1:0"}, out, out) }()
	m := out.wait(t, regexp.MustCompile(tm+`relay ready key ([0-9a-f]{64}) listen (127\.0\.0\.1:\d+)$`), 10*time.Second)
	out.wait(t, line(fmtID, shortID, ""), 0)
	key, _ := hex.DecodeString(m[1])
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	defer rcancel()
	res, err := kolorelay.Reserve(rctx, m[2], key, ident(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	res.Close()
	cancel()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("exit %d", c)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not return")
	}
	out.wait(t, line(fmtStop, "", ""), 0)
	out.wait(t, line(fmtStopped, "", ""), 0)
}
