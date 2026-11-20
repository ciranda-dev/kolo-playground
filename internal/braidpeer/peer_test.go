package braidpeer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestParseRefuses(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--listen", "0.0.0.0:0"}, "127.0.0.1"},
		{[]string{"--listen", ":0"}, "127.0.0.1"},
		{[]string{"--listen", "[::1]:0"}, "127.0.0.1"},
		{[]string{"--listen", "192.168.1.2:0"}, "127.0.0.1"},
		{[]string{"--first", "--join"}, "exclude"},
		{[]string{"--quorum", "half"}, "majority or twothirds"},
		{[]string{"--role", "evil"}, "honest or hostile"},
		{[]string{"--bootstrap", "10.0.0.1:7447"}, "127.0.0.1"},
		{[]string{"--bootstrap", "[::1]:7447"}, "127.0.0.1"},
		{[]string{"--pin", "zz"}, "bad public key"},
		{[]string{"extra"}, "unexpected argument"},
	} {
		var errw bytes.Buffer
		_, err := parse(tc.args, &errw)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: error %v, want %q", tc.args, err, tc.want)
		}
	}
	c, err := parse([]string{"--first", "--quorum", "twothirds", "--pulse-gap", "20ms", "--peer", "a,b", "--bootstrap", "127.0.0.1:1"}, io.Discard)
	if err != nil || !c.first || c.group.Quorum != 2 || c.limits.PulseGap != 20*time.Millisecond || len(c.peers) != 2 || len(c.bootstrap) != 1 {
		t.Fatalf("parse: %+v %v", c, err)
	}
}

// session runs a peer in this process on pipes and returns a function that sends a command and returns its reply.
func session(t *testing.T, args ...string) (map[string]any, func(cmd string) (bool, json.RawMessage, string)) {
	r, do, _ := sessionStop(t, args...)
	return r, do
}

// sessionStop is session with a function that quits the peer and waits for it to exit.
func sessionStop(t *testing.T, args ...string) (ready map[string]any, do func(cmd string) (ok bool, data json.RawMessage, errText string), stop func()) {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	done := make(chan int, 1)
	var errw bytes.Buffer
	go func() {
		done <- Run(context.Background(), append([]string{"--dir", filepath.Join(t.TempDir(), "data")}, args...), inR, outW, &errw)
		outW.Close()
		inR.Close()
	}()
	sc := bufio.NewScanner(outR)
	sc.Buffer(make([]byte, 0, 1<<16), 1<<24)
	lines := make(chan map[string]json.RawMessage, 16)
	go func() {
		for sc.Scan() {
			var m map[string]json.RawMessage
			if json.Unmarshal(sc.Bytes(), &m) == nil {
				lines <- m
			}
		}
		close(lines)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			fmt.Fprintln(inW, "0 quit")
			inW.Close()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Error("the peer did not exit")
			}
		})
	}
	t.Cleanup(stop)
	next := func() map[string]json.RawMessage {
		select {
		case m, ok := <-lines:
			if !ok {
				t.Fatalf("peer exited: %s", errw.String())
			}
			return m
		case <-time.After(30 * time.Second):
			t.Fatal("no line from the peer")
			return nil
		}
	}
	var r struct{ Ev string }
	first := next()
	json.Unmarshal(first["ev"], &r.Ev)
	if r.Ev != "ready" {
		t.Fatalf("first line %v", first)
	}
	ready = map[string]any{}
	for k, v := range first {
		var x any
		json.Unmarshal(v, &x)
		ready[k] = x
	}
	n := 0
	do = func(cmd string) (bool, json.RawMessage, string) {
		n++
		fmt.Fprintf(inW, "%d %s\n", n, cmd)
		for {
			m := next()
			var ev, id string
			json.Unmarshal(m["ev"], &ev)
			json.Unmarshal(m["id"], &id)
			if ev != "reply" || id != fmt.Sprint(n) {
				continue
			}
			var ok bool
			var e string
			json.Unmarshal(m["ok"], &ok)
			json.Unmarshal(m["err"], &e)
			return ok, m["data"], e
		}
	}
	return ready, do, stop
}

func TestFounderCommands(t *testing.T) {
	ready, do := session(t, "--first", "--name", "f", "--pulse-gap", "10ms", "--pivot-wait", "100ms")
	for _, k := range []string{"key", "addr", "group", "dir"} {
		if s, _ := ready[k].(string); s == "" {
			t.Errorf("ready line lacks %s: %v", k, ready)
		}
	}
	if addr, _ := ready["addr"].(string); !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Errorf("listens on %v", ready["addr"])
	}
	if ok, _, e := do("put a 1"); !ok {
		t.Fatal(e)
	}
	if ok, _, e := do("putn n 10 16"); !ok {
		t.Fatal(e)
	}
	var st Status
	deadline := time.Now().Add(30 * time.Second)
	for st.SettledData < 11 {
		if time.Now().After(deadline) {
			t.Fatalf("settled %d of 11: %+v", st.SettledData, st)
		}
		_, d, _ := do("status")
		st = Status{}
		if err := json.Unmarshal(d, &st); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st.Name != "f" || !st.HasBraid || st.Seats != 1 || st.Order == "" || st.OrderAt != st.Settled || st.Content == "" {
		t.Fatalf("status: %+v", st)
	}
	if st.OwnCommits == 0 || st.Commits < st.OwnCommits || st.ReplicaCommits != 0 {
		t.Errorf("commit counters: %+v", st)
	}
	_, d, _ := do("get a")
	if !strings.Contains(string(d), `"settled":"1"`) {
		t.Errorf("get a: %s", d)
	}
	// the order digest of a prefix is the one status reports once the prefix is the whole settled order
	_, d, _ = do(fmt.Sprintf("prefix %d", st.Settled))
	var p struct {
		Order string
		At    uint64
	}
	json.Unmarshal(d, &p)
	if p.Order != st.Order || p.At != st.Settled {
		t.Errorf("prefix %+v, status %q at %d", p, st.Order, st.Settled)
	}
	_, d2, _ := do("prefix 2")
	var p2 struct{ Order string }
	json.Unmarshal(d2, &p2)
	if p2.Order == "" || p2.Order == p.Order {
		t.Errorf("a shorter prefix gave %q", p2.Order)
	}
	if ok, _, _ := do("junk 3 50"); !ok {
		t.Error("junk refused")
	}
	if ok, _, e := do("badhead 127.0.0.1:1@00"); ok || !strings.Contains(e, "hostile") {
		t.Errorf("badhead on an honest peer: %v %q", ok, e)
	}
	tooBig := fmt.Sprint(maxValueSize + 1)
	for _, cmd := range []string{"putn big 1 " + tooBig, "putn big 1 -1", "junk 1 " + tooBig, "junk 1 0"} {
		if ok, _, e := do(cmd); ok || !strings.Contains(e, "bad") {
			t.Errorf("%s: %v %q", cmd, ok, e)
		}
	}
	if ok, _, e := do(fmt.Sprintf("putn big 1 %d", maxValueSize)); ok || strings.Contains(e, "bad size") {
		t.Errorf("a value of the largest size: %v %q", ok, e)
	}
	if ok, _, e := do("frobnicate"); ok || !strings.Contains(e, "unknown command") {
		t.Errorf("unknown command: %v %q", ok, e)
	}
	if ok, _, e := do("block 127.0.0.1:9"); !ok {
		t.Errorf("block: %q", e)
	}
	if ok, _, e := do("link 127.0.0.1:9@" + strings.Repeat("00", 32)); ok || !strings.Contains(e, "blocked") {
		t.Errorf("link to a blocked address: %v %q", ok, e)
	}
	if ok, _, e := do("unblock 127.0.0.1:9"); !ok {
		t.Errorf("unblock: %q", e)
	}
}

// TestCommandsRunAtMostSlots feeds more commands than slots and checks that no more than slots run at once and that
// every command is answered.
func TestCommandsRunAtMostSlots(t *testing.T) {
	const slots, total = 4, 20
	var out bytes.Buffer
	p := &peer{out: &out}
	var running, peak atomic.Int32
	full := make(chan struct{}, 1)
	release := make(chan struct{})
	run := func(string, []string) (any, error) {
		n := running.Add(1)
		defer running.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		if n == slots {
			select {
			case full <- struct{}{}:
			default:
			}
		}
		<-release
		return nil, nil
	}
	var in strings.Builder
	for i := range total {
		fmt.Fprintf(&in, "%d wait\n", i)
	}
	done := make(chan struct{})
	go func() {
		p.commands(strings.NewReader(in.String()), func() {}, run, slots)
		close(done)
	}()
	<-full
	time.Sleep(50 * time.Millisecond) // a fifth command, if one could start, has had time to
	if n := running.Load(); n != slots {
		t.Fatalf("%d commands running, want %d", n, slots)
	}
	close(release)
	<-done
	if peak.Load() != slots {
		t.Fatalf("at most %d at once, want %d", peak.Load(), slots)
	}
	if n := strings.Count(out.String(), `"ev":"reply"`); n != total {
		t.Fatalf("%d replies, want %d", n, total)
	}
}

func TestStatusLines(t *testing.T) {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	done := make(chan int, 1)
	go func() {
		done <- Run(context.Background(), []string{"--dir", filepath.Join(t.TempDir(), "d"), "--first", "--status-every", "20ms"}, inR, outW, io.Discard)
		outW.Close()
	}()
	sc := bufio.NewScanner(outR)
	sc.Buffer(make([]byte, 0, 1<<16), 1<<24)
	seen := 0
	for sc.Scan() && seen < 3 {
		var m struct {
			Ev       string
			HasBraid bool
			Content  string
			Order    string
		}
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		if m.Ev == "status" {
			if !m.HasBraid || m.Content == "" || m.Order == "" {
				t.Fatalf("status line: %s", sc.Bytes())
			}
			seen++
		}
	}
	if seen < 3 {
		t.Fatalf("%d status lines", seen)
	}
	inW.Close()
	go io.Copy(io.Discard, outR)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit %d after stdin closed", code)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the peer did not exit when stdin closed")
	}
}

func TestHostileCommandsNeedHostileRole(t *testing.T) {
	_, do := session(t, "--first", "--role", "hostile")
	if ok, _, e := do("deleteown tail 1"); ok || !strings.Contains(e, "write an entry first") {
		t.Errorf("deleteown before a write: %v %q", ok, e)
	}
	if ok, _, e := do("badhead 127.0.0.1:1@00"); ok || strings.Contains(e, "only a hostile") {
		t.Errorf("a hostile peer was refused its own command: %q", e)
	}
}

func TestJoinPrintsRequest(t *testing.T) {
	ready, do := session(t, "--join")
	if s, _ := ready["request"].(string); len(s) < 100 {
		t.Fatalf("no join request: %v", ready)
	}
	var st Status
	_, d, _ := do("status")
	json.Unmarshal(d, &st)
	if st.HasBraid {
		t.Error("a joining peer has a braid")
	}
	if ok, _, e := do("put a 1"); ok || !strings.Contains(e, "no braid") {
		t.Errorf("put before the join: %v %q", ok, e)
	}
}

func TestOpenKeepsIdentityAndData(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	r1, do1, stop1 := sessionStop(t, "--dir", dir, "--first")
	if ok, _, e := do1("put k v"); !ok {
		t.Fatal(e)
	}
	waitSettled(t, do1, 1)
	stop1()
	r2, do2, _ := sessionStop(t, "--dir", dir)
	if r2["key"] != r1["key"] || r2["group"] != r1["group"] {
		t.Errorf("reopened as another peer: %v %v", r1, r2)
	}
	_, d, _ := do2("get k")
	if !strings.Contains(string(d), `"settled":"v"`) {
		t.Errorf("data lost across a restart: %s", d)
	}
	var st Status
	_, d, _ = do2("status")
	json.Unmarshal(d, &st)
	if st.Unconfirmed || st.StrandLost {
		t.Errorf("one-device reopen: %+v", st)
	}
}

func waitSettled(t *testing.T, do func(string) (bool, json.RawMessage, string), n uint64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, d, _ := do("status")
		var st Status
		json.Unmarshal(d, &st)
		if st.SettledData >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("settled %d of %d", st.SettledData, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCountingBackendClassifiesCommits(t *testing.T) {
	own, other := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	var c counters
	b := &backend{Backend: kolostore.NewMemory(), own: own, c: &c}
	key := func(k []byte, tail string) []byte { return append(append([]byte{strandPrefix}, k...), tail...) }
	bt := b.NewBatch()
	bt.Put(key(own, "a"), []byte("x"))
	bt.Put([]byte("view"), []byte("y"))
	if err := bt.Commit(); err != nil {
		t.Fatal(err)
	}
	bt = b.NewBatch()
	bt.Put(key(other, "b"), []byte("123456"))
	bt.Put(key(other, "c"), []byte("1"))
	if err := bt.Commit(); err != nil {
		t.Fatal(err)
	}
	bt = b.NewBatch()
	bt.Put([]byte("meta"), []byte("z"))
	bt.Commit()
	if err := b.Put([]byte("direct"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if c.commits.Load() != 4 || c.ownCommits.Load() != 1 || c.replicaCommits.Load() != 1 {
		t.Errorf("commits %d own %d replica %d", c.commits.Load(), c.ownCommits.Load(), c.replicaCommits.Load())
	}
	if want := int64(len(key(other, "b")) + 6 + len(key(other, "c")) + 1); c.replicaBytes.Load() != want {
		t.Errorf("replica bytes %d, want %d", c.replicaBytes.Load(), want)
	}
	// a batch that fails is not counted
	bt = b.NewBatch()
	bt.Put(key(other, "d"), []byte("1"))
	b.Close()
	if bt.Commit() == nil || c.replicaCommits.Load() != 1 {
		t.Errorf("a failed commit was counted: %d", c.replicaCommits.Load())
	}
}

func TestPacketConnCounts(t *testing.T) {
	a, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var ca, cb counters
	pa, pb := &packetConn{PacketConn: a, c: &ca}, &packetConn{PacketConn: b, c: &cb}
	if _, err := pa.WriteTo([]byte("hello"), b.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	b.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, _, err := pb.ReadFrom(buf); err != nil || n != 5 {
		t.Fatalf("read %d %v", n, err)
	}
	if ca.udpOut.Load() != 1 || ca.udpOutBytes.Load() != 5 || cb.udpIn.Load() != 1 || cb.udpInBytes.Load() != 5 || ca.udpIn.Load() != 0 {
		t.Errorf("counters a %+v b %+v", ca.udpOut.Load(), cb.udpIn.Load())
	}
}
