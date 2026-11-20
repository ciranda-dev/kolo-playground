package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	kolodht "github.com/ciranda-dev/kolo-dht"
	koloid "github.com/ciranda-dev/kolo-id"
	koloswarm "github.com/ciranda-dev/kolo-swarm"
)

// TestMain runs the test binary as kolo-peer when a test re-executes it, so tests drive a real process.
func TestMain(m *testing.M) {
	if os.Getenv("KOLO_PEER_TEST_MAIN") == "1" {
		main()
	}
	os.Exit(m.Run())
}

// checkGoroutines fails the test if goroutines started during it outlive its cleanups.
func checkGoroutines(t *testing.T) {
	t.Helper()
	before := goroutines()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for {
			var leaked []string
			for id, stack := range goroutines() {
				if _, ok := before[id]; !ok {
					leaked = append(leaked, stack)
				}
			}
			if len(leaked) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("%d goroutines leaked:\n%s", len(leaked), strings.Join(leaked, "\n\n"))
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func goroutines() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := make(map[string]string)
	for _, stack := range strings.Split(string(buf), "\n\n") {
		if id, _, ok := strings.Cut(strings.TrimPrefix(stack, "goroutine "), " "); ok {
			out[id] = stack
		}
	}
	return out
}

func ident(t *testing.T) *koloid.Identity {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// newServer starts a kolo-dht server on loopback with kolo-dht's defaults, as kolo-node runs it.
func newServer(t *testing.T) netip.AddrPort {
	t.Helper()
	n, err := kolodht.New(context.Background(), "127.0.0.1:0", ident(t), &kolodht.Config{Server: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n.Addr().(*net.UDPAddr).AddrPort()
}

// newSwarm starts a swarm on loopback, bootstrapped from server.
func newSwarm(t *testing.T, server netip.AddrPort, cfg *koloswarm.Config) *koloswarm.Swarm {
	t.Helper()
	s, err := koloswarm.New(context.Background(), "127.0.0.1:0", ident(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Bootstrap(ctx, []netip.AddrPort{server}); err != nil {
		t.Fatal(err)
	}
	return s
}

// connected joins topic on a and then b, and returns each side's first Connected peer.
func connected(t *testing.T, a, b *koloswarm.Swarm) (*koloswarm.Peer, *koloswarm.Peer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	topic := topicOf("test")
	for _, s := range []*koloswarm.Swarm{a, b} {
		if err := s.Join(ctx, topic, nil); err != nil {
			t.Fatal(err)
		}
	}
	return nextConnected(t, ctx, a), nextConnected(t, ctx, b)
}

func nextConnected(t *testing.T, ctx context.Context, s *koloswarm.Swarm) *koloswarm.Peer {
	t.Helper()
	for {
		ev, err := s.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Kind == koloswarm.Connected {
			return ev.Peer
		}
	}
}

// lines collects the lines written to it, safe for concurrent use.
type lines struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	list []string
}

func (l *lines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Write(p)
	for {
		line, err := l.buf.ReadString('\n')
		if err != nil {
			l.buf.WriteString(line)
			return len(p), nil
		}
		l.list = append(l.list, strings.TrimSuffix(line, "\n"))
	}
}

func (l *lines) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.list...)
}

// wait returns the submatches of the first line matching re, failing the test after timeout.
func (l *lines) wait(t *testing.T, re *regexp.Regexp, timeout time.Duration) []string {
	t.Helper()
	return l.waitFrom(t, 0, re, timeout)
}

// waitFrom is wait for the lines after the first n.
func (l *lines) waitFrom(t *testing.T, n int, re *regexp.Regexp, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, line := range l.all()[n:] {
			if m := re.FindStringSubmatch(line); m != nil {
				return m
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no line matches %s; got:\n%s", re, strings.Join(l.all(), "\n"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (l *lines) count(re *regexp.Regexp) int {
	n := 0
	for _, line := range l.all() {
		if re.MatchString(line) {
			n++
		}
	}
	return n
}

// goroutineWith returns the ID of a goroutine whose stack holds frame.
func goroutineWith(frame string) string {
	for id, stack := range goroutines() {
		if strings.Contains(stack, frame) {
			return id
		}
	}
	return ""
}

// startedBy returns the stacks of the running goroutines that goroutine id started.
func startedBy(id string) []string {
	var out []string
	for _, stack := range goroutines() {
		if strings.Contains(stack, " in goroutine "+id+"\n") {
			out = append(out, stack)
		}
	}
	return out
}

func (l *lines) has(re *regexp.Regexp) bool {
	for _, line := range l.all() {
		if re.MatchString(line) {
			return true
		}
	}
	return false
}

// proc is a kolo-peer process: a re-execution of the test binary.
type proc struct {
	cmd    *exec.Cmd
	out    *lines
	stderr *lines
	tmp    string
	done   chan struct{}
	err    error
}

// start runs kolo-peer with args, with TMPDIR in a new temp dir so its temporary key lands there.
func start(t *testing.T, args ...string) *proc {
	t.Helper()
	return startIn(t, nil, args...)
}

// startIn is start through wrap, a command that gets the kolo-peer command line as its arguments.
func startIn(t *testing.T, wrap []string, args ...string) *proc {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := &proc{out: &lines{}, stderr: &lines{}, tmp: t.TempDir(), done: make(chan struct{})}
	cmdline := append(append(wrap, exe), args...)
	p.cmd = exec.Command(cmdline[0], cmdline[1:]...)
	p.cmd.Env = append(os.Environ(), "KOLO_PEER_TEST_MAIN=1", "TMPDIR="+p.tmp)
	p.cmd.Dir = t.TempDir()
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = p.stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	copied := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			io.WriteString(p.out, sc.Text()+"\n")
		}
		close(copied)
	}()
	go func() {
		<-copied
		p.err = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		p.cmd.Process.Kill()
		<-p.done
	})
	return p
}

// stop sends SIGINT and returns the exit code, failing after 15s.
func (p *proc) stop(t *testing.T) int {
	t.Helper()
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		t.Fatal("kolo-peer did not stop within 15s")
	}
	return p.cmd.ProcessState.ExitCode()
}

// tmpEntries lists what the process left in its TMPDIR.
func (p *proc) tmpEntries(t *testing.T) []string {
	t.Helper()
	ents, err := os.ReadDir(p.tmp)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names
}
