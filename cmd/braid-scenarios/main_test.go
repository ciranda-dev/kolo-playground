package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-playground/internal/braidpeer"
)

var long = flag.Bool("long", false, "run the long scenarios (late-joiner-10k) too")

// TestMain runs the test binary as a braid-peer when a test starts one of its processes.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == peerArg {
		os.Exit(braidpeer.Run(context.Background(), os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

// TestPeerModeIsAnArgument checks that the environment does not turn the binary into a peer, and that the first argument does.
func TestPeerModeIsAnArgument(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "KOLO_BRAID_PEER_MAIN=1")
	if b, err := cmd.CombinedOutput(); err != nil || !strings.Contains(string(b), "PASS") {
		t.Fatalf("with the old variable set: %v\n%s", err, b)
	}
	b, err := exec.Command(os.Args[0], peerArg, "--listen", "0.0.0.0:0").CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 || !strings.Contains(string(b), "braid-peer:") {
		t.Fatalf("%s as first argument: %v\n%s", peerArg, err, b)
	}
}

func logger(t *testing.T) func(string, ...any) {
	return func(f string, a ...any) { t.Logf(f, a...) }
}

// TestScenarios runs every scenario of the plan; the long ones need -long.
func TestScenarios(t *testing.T) {
	for _, sc := range scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			if sc.Long && !*long {
				t.Skip("long scenario: pass -long")
			}
			rep := Execute(context.Background(), sc, 20*time.Minute, logger(t))
			var out bytes.Buffer
			rep.Print(&out)
			t.Log("\n" + out.String())
			if rep.Err != nil {
				t.Fatal(rep.Err)
			}
		})
	}
}

// TestPlanCovered checks that every scenario of the test plan has an entry, once.
func TestPlanCovered(t *testing.T) {
	want := []string{"convergence", "partition-heal", "admins-concurrent", "mutual-removal", "writer-fork", "seat-fork-twothirds",
		"spam", "kill9", "restore-behind", "restore-inconsistent", "one-device", "clone-halt", "former-settler-join",
		"late-joiner-10k", "settlers-stopped", "new-settler-offline", "pulsegap-mixed", "discovery"}
	seen := map[string]bool{}
	for _, sc := range scenarios {
		if seen[sc.Name] {
			t.Errorf("scenario %q twice", sc.Name)
		}
		seen[sc.Name] = true
		if sc.Desc == "" || sc.Run == nil {
			t.Errorf("scenario %q has no description or no body", sc.Name)
		}
	}
	for _, n := range want {
		if !seen[n] {
			t.Errorf("scenario %q of the plan is missing", n)
		}
	}
	if len(seen) != len(want) {
		t.Errorf("%d scenarios, the plan has %d", len(seen), len(want))
	}
}

// pids returns the process ids of every process a cluster started.
func pids(c *Cluster) []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int
	for _, p := range c.all {
		out = append(out, p.cmd.Process.Pid)
	}
	return out
}

// TestExecuteKillsEveryProcessOnFailure: a failed scenario leaves no process behind and its failure is the report's error.
func TestExecuteKillsEveryProcessOnFailure(t *testing.T) {
	var started []int
	sc := Scenario{Name: "fails", Desc: "x", Run: func(c *Cluster) {
		c.Grow(3, "k")
		started = pids(c)
		c.Fatalf("on purpose %d", 7)
	}}
	rep := Execute(context.Background(), sc, time.Minute, logger(t))
	if rep.Err == nil || !strings.Contains(rep.Err.Error(), "on purpose 7") {
		t.Fatalf("error %v", rep.Err)
	}
	if len(started) < 3 {
		t.Fatalf("%d processes started", len(started))
	}
	for _, pid := range started {
		deadline := time.Now().Add(10 * time.Second)
		for !gone(pid) {
			if time.Now().After(deadline) {
				t.Fatalf("process %d outlived the scenario", pid)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestExecuteTurnsPanicIntoError: a bug in a scenario is an error, not a crash, and the processes still go.
func TestExecuteTurnsPanicIntoError(t *testing.T) {
	var started []int
	sc := Scenario{Name: "panics", Desc: "x", Run: func(c *Cluster) {
		c.Founder("p")
		started = pids(c)
		panic("a bug in the scenario")
	}}
	rep := Execute(context.Background(), sc, time.Minute, logger(t))
	if rep.Err == nil || !strings.Contains(rep.Err.Error(), "panic") {
		t.Fatalf("error %v", rep.Err)
	}
	for _, pid := range started {
		deadline := time.Now().Add(10 * time.Second)
		for !gone(pid) {
			if time.Now().After(deadline) {
				t.Fatalf("process %d outlived the scenario", pid)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestExecuteTimeout: a scenario that waits for something that never happens fails at its limit and leaves nothing running.
func TestExecuteTimeout(t *testing.T) {
	sc := Scenario{Name: "stuck", Desc: "x", Run: func(c *Cluster) {
		c.Founder("s")
		c.Until("never", time.Hour, func() (bool, string) { return false, "nothing" })
	}}
	t0 := time.Now()
	rep := Execute(context.Background(), sc, 3*time.Second, logger(t))
	if rep.Err == nil || !strings.Contains(rep.Err.Error(), "never") {
		t.Fatalf("error %v", rep.Err)
	}
	if d := time.Since(t0); d > 30*time.Second {
		t.Fatalf("took %v", d)
	}
}

// TestProcessCommands drives one peer process through its commands and checks what each answers.
func TestProcessCommands(t *testing.T) {
	sc := Scenario{Name: "commands", Desc: "x", Run: func(c *Cluster) {
		p := c.Founder("solo")
		c.Until("the founder is its own member", wait, func() (bool, string) {
			st := c.Stat(p)
			return st.HasBraid && st.Seats == 1 && st.Quorum == 1 && len(st.Members) == 1, fmt.Sprintf("%+v", st)
		})
		c.Do(p, "put", "k", "v1")
		c.Wait("settled", []*Proc{p}, 1, wait)
		var g struct{ Settled, View string }
		c.Must(json.Unmarshal(c.Do(p, "get", "k"), &g))
		c.Check(g.Settled == "v1" && g.View == "v1", "get: %+v", g)
		c.Do(p, "del", "k")
		c.Wait("deleted", []*Proc{p}, 2, wait)
		g.Settled, g.View = "", ""
		c.Must(json.Unmarshal(c.Do(p, "get", "k"), &g))
		c.Check(g.Settled == "" && g.View == "", "get after del: %+v", g)
		s := c.Stat(p)
		var pre struct {
			Order string
			At    uint64
		}
		c.Must(json.Unmarshal(c.Do(p, "prefix", "3"), &pre))
		c.Check(pre.At == 3 && pre.Order != "" && s.Settled >= 3, "prefix: %+v at settled %d", pre, s.Settled)
		var whole struct{ Order string }
		c.Must(json.Unmarshal(c.Do(p, "prefix", jsonUint(s.Settled)), &whole))
		c.Check(whole.Order == s.Order, "prefix of the whole settled order differs from status")
		for _, bad := range [][]string{{"nonsense"}, {"put", "onlykey"}, {"prefix", "x"}, {"prefix", "100000"}, {"link", "nowhere"},
			{"link", "10.0.0.1:1@00"}, {"admit", "zz"}, {"complete", "00"}, {"badhead", "x"}, {"deleteown", "1"}, {"removemember", "1"},
			{"settlers", "majority"}} {
			c.Check(c.DoErr(p, bad[0], bad[1:]...) != nil, "%v was accepted", bad)
		}
		c.Check(p.Alive(), "the process died on a bad command")
		c.Check(c.Stat(p).Halted == "", "bad commands halted the braid")
	}}
	if rep := Execute(context.Background(), sc, 2*time.Minute, logger(t)); rep.Err != nil {
		t.Fatal(rep.Err)
	}
}

func jsonUint(n uint64) string { b, _ := json.Marshal(n); return string(b) }

// TestTapMeasuresAnnouncements: the tap, a pin, receives the announcements and nothing else breaks a link to it.
func TestTapMeasuresAnnouncements(t *testing.T) {
	sc := Scenario{Name: "tap", Desc: "x", Run: func(c *Cluster) {
		ps := c.Grow(3, "t")
		c.Do(ps[1], "putn", "x", "10")
		c.Wait("settled", ps, 0, wait)
		s := c.tap.Stats()
		c.Check(s.Links >= 3, "tap links %d", s.Links)
		c.Check(s.AnnounceMsgs > 0 && s.AnnounceBytes > s.AnnounceMsgs*40, "announce %d msgs %d bytes", s.AnnounceMsgs, s.AnnounceBytes)
		c.Check(s.Undecodable == 0, "%d messages the tap could not decode", s.Undecodable)
		st := c.Stat(ps[0])
		c.Check(st.UDPOutBytes > 0 && st.UDPInBytes > 0 && st.UDPOut > 0, "udp counters %+v", st)
		c.Check(st.ReplicaCommits > 0 && st.OwnCommits > 0 && st.Commits >= st.ReplicaCommits, "commit counters %+v", st)
	}}
	if rep := Execute(context.Background(), sc, 2*time.Minute, logger(t)); rep.Err != nil {
		t.Fatal(rep.Err)
	}
}

// TestPartitionBlocksLinks: a cut stops traffic between two groups and nothing else, and a heal restores it.
func TestPartitionBlocksLinks(t *testing.T) {
	sc := Scenario{Name: "cut", Desc: "x", Run: func(c *Cluster) {
		ps := c.Grow(3, "c")
		c.Cut(ps[:1], ps[1:])
		c.Until("links drop", wait, func() (bool, string) { return c.Stat(ps[0]).Links == 1, "" }) // only the tap is left
		c.Check(c.DoErr(ps[0], "link", ps[1].Target()) != nil, "a blocked address was dialed")
		c.Until("the other two keep their link", wait, func() (bool, string) { return c.Stat(ps[1]).Links == 2, "" }) // ps[1] sees the cut a moment after ps[0]
		c.Check(c.Stat(ps[1]).Links == 2, "links between the other two changed")
		c.Do(ps[0], "put", "alone", "1")
		holds(c, "tentative write", ps[:1], c.Data(ps[0])+1)
		c.Check(c.Stat(ps[1]).TentativeData == c.Stat(ps[1]).SettledData, "the write crossed the cut")
		c.Heal(ps...)
		c.Wait("agree after the heal", ps, 0, wait)
		c.Check(c.Stat(ps[1]).Links == 3, "links after the heal: %d", c.Stat(ps[1]).Links)
	}}
	if rep := Execute(context.Background(), sc, 2*time.Minute, logger(t)); rep.Err != nil {
		t.Fatal(rep.Err)
	}
}

// TestReportPrints checks the cost report has every cost the plan asks for.
func TestReportPrints(t *testing.T) {
	r := &Report{Name: "x", Duration: time.Second, Timings: []Timing{{"settle", 1500 * time.Millisecond}},
		Costs: Costs{Peers: 3, Tap: TapStats{AnnounceMsgs: 4, AnnounceBytes: 2048, Links: 2}, ReplicaCommits: 9,
			Admit: []time.Duration{3 * time.Millisecond}, JoinTrip: []time.Duration{9 * time.Millisecond}}}
	var b bytes.Buffer
	r.Print(&b)
	for _, want := range []string{"scenario x: ok", "settle", "1.5s", "announce 4 msgs 2.0 KiB over 2 tap links (1.0 KiB per link)",
		"9 replica commits", "Admit call 3ms", "round trip 9ms"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("report lacks %q:\n%s", want, b.String())
		}
	}
	r.Err = context.Canceled
	b.Reset()
	r.Print(&b)
	if !strings.Contains(b.String(), "FAILED") || !strings.Contains(b.String(), "context canceled") {
		t.Errorf("failed report: %s", b.String())
	}
}

// TestRunFlags checks the command line: list, run filter and the long switch.
func TestRunFlags(t *testing.T) {
	var out, errw bytes.Buffer
	if code := run([]string{"-list"}, &out, &errw); code != 0 {
		t.Fatalf("list exit %d: %s", code, errw.String())
	}
	if !strings.Contains(out.String(), "one-device") || !strings.Contains(out.String(), "(long)") {
		t.Fatalf("list: %s", out.String())
	}
	if code := run([]string{"-run", "("}, &out, &errw); code != 2 {
		t.Fatalf("bad regexp exit %d", code)
	}
	out.Reset()
	if code := run([]string{"-run", "no-such-scenario"}, &out, &errw); code != 1 || !strings.Contains(out.String(), "0 scenarios") {
		t.Fatalf("no match: exit %d %s", code, out.String())
	}
}
