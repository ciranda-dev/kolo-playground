package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	koloid "github.com/ciranda-dev/kolo-id"
)

// Line formats, shared by the process tests and the README check. ID stands for a short ID.
const (
	tm              = `^\d\d:\d\d:\d\d `
	shortID         = `([0-9a-f]{8})`
	fmtID           = tm + `id ` + shortID + `$`
	fmtIDV          = tm + `id ` + shortID + ` key [0-9a-f]{64} listen \S+$`
	fmtBoot         = tm + `bootstrap reached$`
	fmtJoined       = tm + `joined topic "TOPIC"$`
	fmtFound        = tm + `peer found ID$`
	fmtConn         = tm + `peer connected ID direct$`
	fmtConnR        = tm + `peer connected ID relayed$`
	fmtConnV        = tm + `peer connected ID direct at 127\.0\.0\.1:\d+$`
	fmtPing         = tm + `ping ID rtt \d+\.\dms$`
	fmtLost         = tm + `peer lost ID: koloswarm: connection lost$`
	fmtUnreach      = tm + `peer unreachable ID: hole punching failed, no relay, retrying$`
	fmtUnreachR     = tm + `peer unreachable ID: hole punching failed, trying the relay$`
	fmtUnreachBusy  = tm + `peer unreachable ID: hole punching failed, relay busy, retrying$`
	fmtReserved     = tm + `relay reserved$`
	fmtEnded        = tm + `relay reservation ended: kolorelay: reservation ended: kolotransport: stream reset by peer \(code 2\)$`
	fmtReady        = tm + `relay ready key [0-9a-f]{64} listen \S+$`
	fmtRelayFailed  = tm + `relay ID failed: kolorelay: target unavailable through the relay$`
	fmtRelayTimeout = tm + `relay ID failed: context deadline exceeded$`
	fmtStop         = tm + `stopping$`
	fmtStopped      = tm + `stopped$`
)

// line compiles a format with ID and TOPIC filled in.
func line(format, id, topic string) *regexp.Regexp {
	return regexp.MustCompile(strings.NewReplacer("ID", id, "TOPIC", regexp.QuoteMeta(topic)).Replace(format))
}

func TestTopic(t *testing.T) {
	for name, want := range map[string]string{
		"demo": "1f8c1d5e6d722c95994fdad6fef7ab2f646428813b3ea58d0e4f4e8e89386241",
		"test": "d965df7c5a7fb531436fe0dcb43d9871dfdeaaa75ada79b877529cb313cc9016",
	} {
		got := topicOf(name)
		if hex.EncodeToString(got[:]) != want {
			t.Errorf("topicOf(%q) = %x, want %s", name, got, want)
		}
	}
}

func TestFlags(t *testing.T) {
	const b = "203.0.113.5:7447"
	nine := []string{"--topic", "x"}
	for range 9 {
		nine = append(nine, "--bootstrap", b)
	}
	peer := []string{"--bootstrap", b, "--topic", "x"}
	with := func(args ...string) []string { return append(append([]string(nil), peer...), args...) }
	const ipv6 = "IPv6 input is refused; want an IPv4 IP:PORT"
	const badKey = "want IP:PORT@KEY, KEY the relay's public key in 64 hex digits"
	for _, tc := range []struct {
		args []string
		code int
		msg  string
	}{
		{nil, 2, "kolo-peer: --bootstrap is required"},
		{[]string{"--bootstrap", b}, 2, "kolo-peer: --topic is required"},
		{[]string{"--bootstrap", "nope", "--topic", "x"}, 2, `invalid value "nope" for flag -bootstrap: want IP:PORT`},
		{[]string{"--bootstrap", "example.org:7447", "--topic", "x"}, 2, "want IP:PORT"},
		{[]string{"--bootstrap", "203.0.113.5:0", "--topic", "x"}, 2, "want IP:PORT"},
		{[]string{"--bootstrap", "[::1]:7447", "--topic", "x"}, 2, `invalid value "[::1]:7447" for flag -bootstrap: ` + ipv6},
		{[]string{"--bootstrap", "[2001:db8::1]:7447", "--topic", "x"}, 2, ipv6},
		{[]string{"--bootstrap", "[::ffff:203.0.113.5]:7447", "--topic", "x"}, 2, ipv6},
		{[]string{"--bootstrap", "[::ffff:127.0.0.2]:7447", "--topic", "x", "--allow-loopback"}, 2, ipv6},
		{nine, 2, "kolo-peer: at most 8 --bootstrap"},
		{[]string{"--bootstrap", b, "--topic", strings.Repeat("a", 257)}, 2, "kolo-peer: --topic must be at most 256 bytes of UTF-8"},
		{[]string{"--bootstrap", b, "--topic", "\xff"}, 2, "kolo-peer: --topic must be at most 256 bytes of UTF-8"},
		{with("--max-peers", "0"), 2, "kolo-peer: --max-peers must be between 1 and 32"},
		{with("--max-peers", "33"), 2, "kolo-peer: --max-peers must be between 1 and 32"},
		{with("--listen", "nope"), 2, `invalid value "nope" for flag -listen: want IP:PORT`},
		{with("--listen", "localhost:0"), 2, "want IP:PORT"},
		{with("--listen", "[::]:0"), 2, `invalid value "[::]:0" for flag -listen: ` + ipv6},
		{with("--listen", "[::1]:7447"), 2, ipv6},
		{with("--listen", "[::ffff:0.0.0.0]:0"), 2, ipv6},
		{with("--relay"), 2, "flag needs an argument: -relay"},
		{with("--relay", "nope"), 2, `invalid value "nope" for flag -relay: want IP:PORT`},
		{with("--relay", "203.0.113.9:7448"), 2, badKey},
		{with("--relay", "203.0.113.9:7448@"+relayKeyHex[:62]), 2, badKey},
		{with("--relay", "203.0.113.9:7448@"+strings.Repeat("z", 64)), 2, badKey},
		{with("--relay", "203.0.113.9:0@"+relayKeyHex), 2, "want IP:PORT"},
		{with("--relay", "[::1]:7448@"+relayKeyHex), 2, ipv6},
		{with("--relay", "[::ffff:203.0.113.9]:7448@"+relayKeyHex), 2, ipv6},
		{with("--relay", "127.0.0.1:7448@"+relayKeyHex), 2,
			"kolo-peer: --relay 127.0.0.1:7448 is a loopback address; add --allow-loopback to run peers on one machine"},
		{with("extra"), 2, "kolo-peer: unexpected argument"},
		{[]string{"--bootstrap", "127.0.0.1:7447", "--topic", "x"}, 2,
			"kolo-peer: --bootstrap 127.0.0.1:7447 is a loopback address; add --allow-loopback to run peers on one machine"},
		{[]string{"--bootstrap", b, "--bootstrap", "127.0.0.3:7447", "--topic", "x"}, 2, "kolo-peer: --bootstrap 127.0.0.3:7447 is a loopback address"},
		{[]string{"--serve-relay", "--topic", "x"}, 2, "kolo-peer: --serve-relay takes only --listen, --key and --verbose"},
		{[]string{"--serve-relay", "--bootstrap", b}, 2, "kolo-peer: --serve-relay takes only --listen, --key and --verbose"},
		{[]string{"--serve-relay", "--allow-loopback"}, 2, "kolo-peer: --serve-relay takes only --listen, --key and --verbose"},
		{[]string{"--serve-relay", "--listen", "[::]:7448"}, 2, ipv6},
		{[]string{"-h"}, 0, "-relay IP:PORT@KEY"},
	} {
		// A loopback --listen first, so that even a case that wrongly passes the checks binds only 127.0.0.1.
		args := append([]string{"--listen", "127.0.0.1:0"}, tc.args...)
		var out, errb bytes.Buffer
		code := run(context.Background(), args, &out, &errb)
		if code != tc.code || !strings.Contains(errb.String(), tc.msg) || out.Len() != 0 {
			t.Errorf("%q: exit %d, stderr %q, stdout %q; want %d and %q", tc.args, code, errb.String(), out.String(), tc.code, tc.msg)
		}
	}
}

const relayKeyHex = "5e6f7a8b9c0d1e2f30415263748596a7b8c9dae0f1021324354657687980a1b2"

func TestFlagValues(t *testing.T) {
	o, err := parseFlags([]string{"--bootstrap", "203.0.113.5:7447", "--topic", "x", "--listen", ":7447",
		"--relay", "203.0.113.9:7448@" + relayKeyHex}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.listen != netip.MustParseAddrPort("0.0.0.0:7447") || o.relay != netip.MustParseAddrPort("203.0.113.9:7448") ||
		hex.EncodeToString(o.relayKey) != relayKeyHex {
		t.Errorf("listen %v, relay %v, key %x", o.listen, o.relay, []byte(o.relayKey))
	}
	if o, err := parseFlags([]string{"--serve-relay", "--listen", "127.0.0.1:7448"}, io.Discard); err != nil || !o.serveRelay {
		t.Errorf("--serve-relay: %v", err)
	}
	for _, args := range [][]string{{"--serve-relay"}, {"--serve-relay", "--verbose"}} {
		var errb bytes.Buffer
		if _, err := parseFlags(args, &errb); err == nil || !strings.HasPrefix(errb.String(), "kolo-peer: --serve-relay needs --listen\n") {
			t.Errorf("%q: %v, stderr %q", args, err, errb.String())
		}
	}
	if o, err := parseFlags([]string{"--bootstrap", "203.0.113.5:7447", "--topic", "x"}, io.Discard); err != nil ||
		o.relay.IsValid() || o.listen != netip.MustParseAddrPort("0.0.0.0:0") {
		t.Errorf("defaults: relay %v, listen %v (%v)", o.relay, o.listen, err)
	}
}

func TestBootstrapFails(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := pc.LocalAddr().String()
	pc.Close()
	var out, errb bytes.Buffer
	code := run(context.Background(), []string{"--bootstrap", dead, "--topic", "x", "--listen", "127.0.0.1:0", "--allow-loopback"}, &out, &errb)
	if code != 1 || errb.String() != "kolo-peer: bootstrap: kolodht: no server answered\n" {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	if !line(fmtID, shortID, "").MatchString(strings.Split(out.String(), "\n")[0]) {
		t.Errorf("stdout %q", out.String())
	}
	if ents, _ := os.ReadDir(tmp); len(ents) != 0 {
		t.Errorf("temporary key directory left: %v", ents)
	}
}

func TestAllow(t *testing.T) {
	for _, tc := range []struct {
		addr     string
		loopback bool
	}{
		{"127.0.0.1:1", true},
		{"127.1.2.3:1", true},
		{"192.168.1.2:1", false},
		{"169.254.1.2:1", false},
		{"203.0.113.5:1", false},
	} {
		ap := netip.MustParseAddrPort(tc.addr)
		if got := allow(false)(nil, ap); got == tc.loopback {
			t.Errorf("allow(false)(%s) = %v", ap, got)
		}
		if !allow(true)(nil, ap) {
			t.Errorf("allow(true)(%s) = false", ap)
		}
	}
	if !allow(false)(nil, netip.AddrPort{}) {
		t.Error("allow(false) refused the zero AddrPort")
	}
}

func TestIdentityTemporary(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	id, cleanup, err := identity("")
	if err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(tmp)
	if err != nil || len(ents) != 1 || !strings.HasPrefix(ents[0].Name(), "kolo-peer-") {
		t.Fatalf("temp dir holds %v (%v)", ents, err)
	}
	loaded, err := koloid.Load(filepath.Join(tmp, ents[0].Name(), "peer.key"))
	if err != nil || !bytes.Equal(loaded.PublicKey(), id.PublicKey()) {
		t.Fatalf("temporary key: %v", err)
	}
	cleanup()
	if ents, _ := os.ReadDir(tmp); len(ents) != 0 {
		t.Errorf("cleanup left %v", ents)
	}
}

func TestIdentityFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys", "peer")
	a, _, err := identity(path)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := identity(path)
	if err != nil || !bytes.Equal(a.PublicKey(), b.PublicKey()) {
		t.Fatalf("second load: %v", err)
	}
	if runtime.GOOS == "windows" {
		return
	}
	for p, want := range map[string]os.FileMode{path: 0o600, filepath.Dir(path): 0o700} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s: mode %v (%v), want %v", p, fi.Mode().Perm(), err, want)
		}
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := identity(path); !errors.Is(err, koloid.ErrInsecureKeyFile) {
		t.Errorf("0644 key file: %v, want ErrInsecureKeyFile", err)
	}
}

// TestRunStops runs two peers in this process, waits for a connection and a ping each way, and stops them.
func TestRunStops(t *testing.T) {
	checkGoroutines(t)
	t.Setenv("TMPDIR", t.TempDir())
	server := newServer(t).String()
	args := []string{"--bootstrap", server, "--topic", "test", "--listen", "127.0.0.1:0", "--allow-loopback"}
	type peer struct {
		out    *lines
		cancel context.CancelFunc
		code   chan int
	}
	launch := func() peer {
		ctx, cancel := context.WithCancel(context.Background())
		p := peer{&lines{}, cancel, make(chan int, 1)}
		go func() { p.code <- run(ctx, args, p.out, p.out) }()
		t.Cleanup(cancel)
		return p
	}
	a := launch()
	ida := a.out.wait(t, line(fmtID, shortID, ""), 10*time.Second)[1]
	a.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	b := launch()
	idb := b.out.wait(t, line(fmtID, shortID, ""), 10*time.Second)[1]
	for _, c := range []struct {
		p     peer
		other string
	}{{a, idb}, {b, ida}} {
		c.p.out.wait(t, line(fmtConn, c.other, ""), 20*time.Second)
		c.p.out.wait(t, line(fmtPing, c.other, ""), 20*time.Second)
	}
	for i, p := range []peer{a, b} {
		p.cancel()
		select {
		case code := <-p.code:
			if code != 0 {
				t.Errorf("exit %d", code)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("run did not return")
		}
		p.out.wait(t, line(fmtStop, "", ""), 0)
		p.out.wait(t, line(fmtStopped, "", ""), 0)
		if i == 0 {
			b.out.wait(t, line(fmtLost, ida, ""), 20*time.Second)
		}
	}
}

func processTest(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts processes")
	}
	if runtime.GOOS == "windows" {
		t.Skip("uses SIGINT")
	}
}

// TestPeersConnect starts two kolo-peer processes against a loopback kolo-dht server, as README case 1 does.
func TestPeersConnect(t *testing.T) {
	processTest(t)
	server := newServer(t).String()
	args := []string{"--bootstrap", server, "--topic", "test", "--listen", "127.0.0.1:0", "--allow-loopback"}
	a := start(t, append(args, "--verbose")...)
	ida := a.out.wait(t, line(fmtIDV, shortID, ""), 10*time.Second)[1]
	a.out.wait(t, line(tm+`joined topic "test" [0-9a-f]{64}$`, "", ""), 10*time.Second)
	b := start(t, args...)
	idb := b.out.wait(t, line(fmtID, shortID, ""), 10*time.Second)[1]

	b.out.wait(t, line(fmtBoot, "", ""), 10*time.Second)
	b.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	b.out.wait(t, line(fmtFound, ida, ""), 20*time.Second)
	b.out.wait(t, line(fmtConn, ida, ""), 20*time.Second)
	a.out.wait(t, line(fmtConnV, idb, ""), 20*time.Second)
	b.out.wait(t, line(fmtPing, ida, ""), 20*time.Second)
	a.out.wait(t, line(fmtPing, idb, ""), 20*time.Second)

	hex64 := regexp.MustCompile(`[0-9a-f]{64}`)
	for _, l := range b.out.all() {
		if strings.Contains(l, "127.0.0.1") || hex64.MatchString(l) {
			t.Errorf("non-verbose line shows an address or a full key: %q", l)
		}
	}

	if code := b.stop(t); code != 0 {
		t.Errorf("b exit %d, stderr %q", code, b.stderr.all())
	}
	b.out.wait(t, line(fmtStop, "", ""), 0)
	b.out.wait(t, line(fmtStopped, "", ""), 0)
	if left := b.tmpEntries(t); len(left) != 0 {
		t.Errorf("b left %v in its TMPDIR", left)
	}
	a.out.wait(t, line(tm+`peer lost ID at 127\.0\.0\.1:\d+: koloswarm: connection lost$`, idb, ""), 20*time.Second)
	if code := a.stop(t); code != 0 {
		t.Errorf("a exit %d, stderr %q", code, a.stderr.all())
	}
	if left := a.tmpEntries(t); len(left) != 0 {
		t.Errorf("a left %v in its TMPDIR", left)
	}
	if len(a.stderr.all()) != 0 || len(b.stderr.all()) != 0 {
		t.Errorf("stderr: %q %q", a.stderr.all(), b.stderr.all())
	}
}

// TestSIGHUPCleansKey checks that SIGHUP, sent when a terminal closes, stops kolo-peer like SIGINT.
func TestSIGHUPCleansKey(t *testing.T) {
	processTest(t)
	// Under nohup kolo-peer would inherit an ignored SIGHUP; a handler here makes exec give it the default action.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	p := start(t, "--bootstrap", newServer(t).String(), "--topic", "test", "--listen", "127.0.0.1:0", "--allow-loopback")
	p.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	if len(p.tmpEntries(t)) != 1 {
		t.Fatalf("TMPDIR holds %v, want the temporary key directory", p.tmpEntries(t))
	}
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		t.Fatal("kolo-peer did not stop")
	}
	if code := p.cmd.ProcessState.ExitCode(); code != 0 || !p.out.has(line(fmtStopped, "", "")) {
		t.Errorf("exit %d after SIGHUP; got:\n%s", code, strings.Join(p.out.all(), "\n"))
	}
	if left := p.tmpEntries(t); len(left) != 0 {
		t.Errorf("TMPDIR left with %v", left)
	}
}

// TestSIGHUPIgnored checks that kolo-peer keeps an ignored SIGHUP ignored, as nohup sets it.
func TestSIGHUPIgnored(t *testing.T) {
	processTest(t)
	p := startIn(t, []string{"sh", "-c", `trap "" HUP; exec "$0" "$@"`},
		"--bootstrap", newServer(t).String(), "--topic", "test", "--listen", "127.0.0.1:0", "--allow-loopback")
	p.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
		t.Fatalf("kolo-peer stopped on an ignored SIGHUP; got:\n%s", strings.Join(p.out.all(), "\n"))
	case <-time.After(2 * time.Second):
	}
	if code := p.stop(t); code != 0 {
		t.Errorf("exit %d", code)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

// TestKeyGoneAtStopping checks that the temporary key is removed before stopping is logged, so a second signal leaves
// nothing.
func TestKeyGoneAtStopping(t *testing.T) {
	checkGoroutines(t)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	out := &lines{}
	var left []os.DirEntry
	w := writerFunc(func(b []byte) (int, error) {
		if bytes.HasSuffix(b, []byte(" stopping\n")) {
			left, _ = os.ReadDir(tmp)
		}
		return out.Write(b)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	code := make(chan int, 1)
	go func() {
		code <- run(ctx, []string{"--bootstrap", newServer(t).String(), "--topic", "test", "--listen", "127.0.0.1:0", "--allow-loopback"}, w, w)
	}()
	out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	if ents, _ := os.ReadDir(tmp); len(ents) != 1 {
		t.Fatalf("TMPDIR holds %v, want the temporary key directory", ents)
	}
	cancel()
	if c := <-code; c != 0 {
		t.Errorf("exit %d", c)
	}
	out.wait(t, line(fmtStop, "", ""), 0)
	if len(left) != 0 {
		t.Errorf("at stopping, TMPDIR still holds %v", left)
	}
}

// TestPeerUnreachable kills a peer after it announced, so the next peer finds a record nobody answers at.
func TestPeerUnreachable(t *testing.T) {
	processTest(t)
	server := newServer(t).String()
	args := []string{"--bootstrap", server, "--topic", "test", "--listen", "127.0.0.1:0", "--allow-loopback"}
	a := start(t, args...)
	ida := a.out.wait(t, line(fmtID, shortID, ""), 10*time.Second)[1]
	a.out.wait(t, line(fmtJoined, "", "test"), 10*time.Second)
	a.cmd.Process.Kill()
	<-a.done

	b := start(t, args...)
	b.out.wait(t, line(fmtFound, ida, ""), 20*time.Second)
	b.out.wait(t, line(fmtUnreach, ida, ""), 30*time.Second)
	if b.out.has(line(fmtConn, ida, "")) {
		t.Error("connected to a dead peer")
	}
	if code := b.stop(t); code != 0 {
		t.Errorf("b exit %d", code)
	}
}

// TestReadme checks that every log line the README shows has the format the code prints.
func TestReadme(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	var shown []string
	for _, l := range strings.Split(string(readme), "\n") {
		if regexp.MustCompile(tm).MatchString(l) {
			shown = append(shown, l)
		}
	}
	formats := []string{fmtID, fmtBoot, fmtJoined, fmtFound, fmtConn, fmtConnR, fmtPing, fmtLost, fmtUnreach, fmtUnreachR, fmtUnreachBusy,
		fmtReserved, fmtReady, fmtRelayFailed, fmtRelayTimeout, fmtStop, fmtStopped}
	for _, f := range formats {
		re := line(f, shortID, "demo")
		found := false
		for _, l := range shown {
			found = found || re.MatchString(l)
		}
		if !found {
			t.Errorf("README shows no line like %s", re)
		}
	}
	for _, l := range shown {
		ok := false
		for _, f := range formats {
			ok = ok || line(f, shortID, "demo").MatchString(l)
		}
		if !ok {
			t.Errorf("README line %q matches no format", l)
		}
	}
}
