package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	kolodht "github.com/ciranda-dev/kolo-dht"
	koloid "github.com/ciranda-dev/kolo-id"

	"github.com/ciranda-dev/kolo-playground/internal/braidpeer"
)

func hexKey(k []byte) string { return hex.EncodeToString(k) }

// failure is what a scenario panics with when a check fails; Execute turns it into an error.
type failure struct{ msg string }

// Cluster is the processes of one scenario, their temporary directories and the tap that measures them.
type Cluster struct {
	ctx  context.Context
	root string
	tap  *Tap
	logf func(string, ...any)

	// limits are the flags every peer gets; group are the flags of the founder.
	limits []string
	group  []string

	mu    sync.Mutex
	all   []*Proc
	live  map[string]*Proc
	dht   *kolodht.Node
	times []Timing
	notes []string
	admit []time.Duration
	// joinTrip is the time from the Admit command to the end of Complete, as the orchestrator saw it.
	joinTrip []time.Duration
}

// Timing is one measured duration of a scenario.
type Timing struct {
	Name string
	D    time.Duration
}

// fastLimits are the limits of a scenario unless it sets its own: short waits, so that settlement takes a few round trips.
var fastLimits = []string{"--pulse-gap", "50ms", "--pivot-wait", "500ms", "--announce-gap", "20ms", "--confirm-wait", "2s"}

func newCluster(ctx context.Context, logf func(string, ...any)) (*Cluster, error) {
	root, err := os.MkdirTemp("", "braid-scenarios-")
	if err != nil {
		return nil, err
	}
	c := &Cluster{ctx: ctx, root: root, logf: logf, live: map[string]*Proc{}, limits: slices.Clone(fastLimits)}
	return c, nil
}

// start opens the tap once the group flags are known.
func (c *Cluster) start(groupFlags ...string) error {
	c.group = groupFlags
	var err error
	c.tap, err = newTap(c.ctx)
	return err
}

// Close kills every process, closes the tap and the DHT node and removes the temporary directories.
func (c *Cluster) Close() {
	c.mu.Lock()
	all := slices.Clone(c.all)
	c.mu.Unlock()
	for _, p := range all {
		p.Kill()
	}
	if c.tap != nil {
		c.tap.Close()
	}
	if c.dht != nil {
		c.dht.Close()
	}
	os.RemoveAll(c.root)
}

// Fatalf fails the scenario.
func (c *Cluster) Fatalf(format string, a ...any) {
	panic(failure{fmt.Sprintf(format, a...)})
}

// Check fails the scenario unless ok.
func (c *Cluster) Check(ok bool, format string, a ...any) {
	if !ok {
		c.Fatalf(format, a...)
	}
}

// Must fails the scenario on an error.
func (c *Cluster) Must(err error) {
	if err != nil {
		c.Fatalf("%v", err)
	}
}

// Spawn starts a braid-peer named name with the cluster's flags and extra.
func (c *Cluster) Spawn(name string, extra ...string) *Proc {
	args := []string{"--pin", hexKey(c.tap.id.PublicKey())}
	args = append(args, c.limits...)
	if !slices.Contains(extra, "--dir") {
		args = append(args, "--dir", filepath.Join(c.root, name))
	}
	args = append(args, extra...)
	p, err := startProc(c.ctx, name, args)
	if err != nil {
		c.Fatalf("start %s: %v", name, err)
	}
	c.mu.Lock()
	c.all = append(c.all, p)
	c.live[name] = p
	c.mu.Unlock()
	return p
}

// Founder starts the first peer, which creates the braid.
func (c *Cluster) Founder(name string, extra ...string) *Proc {
	p := c.Spawn(name, append(append([]string{"--first"}, c.group...), extra...)...)
	c.attachTap(p)
	return p
}

func (c *Cluster) attachTap(p *Proc) {
	if slices.Contains(p.Args, "--bootstrap") {
		return // a swarm peer is linked by the swarm only
	}
	_, err := p.Do(c.ctx, "link", c.tap.Target())
	c.Must(err)
}

// Target is the ADDR@KEY other peers dial to reach p.
func (p *Proc) Target() string { return p.Addr + "@" + p.Key }

// Do runs a command on p and fails the scenario on an error.
func (c *Cluster) Do(p *Proc, cmd string, args ...string) json.RawMessage {
	d, err := p.Do(c.ctx, cmd, args...)
	c.Must(err)
	return d
}

// DoErr runs a command on p and returns its error.
func (c *Cluster) DoErr(p *Proc, cmd string, args ...string) error {
	_, err := p.Do(c.ctx, cmd, args...)
	return err
}

// Join admits a new peer through inviter and returns it unlinked; opts are Admit's options (default, plain, admin, replace).
func (c *Cluster) Join(inviter *Proc, name string, opts ...string) *Proc {
	return c.JoinWith(inviter, name, nil, opts...)
}

// JoinWith is Join with extra flags for the new process.
func (c *Cluster) JoinWith(inviter *Proc, name string, flags []string, opts ...string) *Proc {
	j := c.Spawn(name, append([]string{"--join"}, flags...)...)
	return c.complete(inviter, j, opts...)
}

// complete admits the joining process j through inviter and completes its join.
func (c *Cluster) complete(inviter, j *Proc, opts ...string) *Proc {
	t0 := time.Now()
	r := admitReq(c, inviter, j, opts...)
	c.Do(j, "complete", r.Welcome)
	c.attachTap(j)
	c.mu.Lock()
	c.admit = append(c.admit, time.Duration(r.Nanos))
	c.joinTrip = append(c.joinTrip, time.Since(t0))
	c.mu.Unlock()
	return j
}

// Place lets p and each of ps reach each other and links p to them.
func (c *Cluster) Place(p *Proc, ps ...*Proc) {
	for _, q := range ps {
		if q.Key == p.Key {
			continue // a copy of one key cannot link to its twin
		}
		c.Do(q, "unblock", p.Addr)
		c.Do(p, "unblock", q.Addr)
		c.Connect(p, q)
	}
}

// Fork stops w, copies its data directory and starts w again; it returns the copy's directory.
func (c *Cluster) Fork(w *Proc, others []*Proc) (*Proc, string) {
	w.Stop(c.ctx)
	dir := c.CopyDir(w, w.Name+"-copy")
	return c.Restart(w, others), dir
}

// Clone starts a second process with the key and data of dir, writing before it is confirmed.
func (c *Cluster) Clone(name, dir string, extra ...string) *Proc {
	p := c.Spawn(name, append([]string{"--dir", dir, "--append-unconfirmed"}, extra...)...)
	c.attachTap(p)
	return p
}

// Connect links a to b: a dials b.
func (c *Cluster) Connect(a, b *Proc) {
	c.Do(a, "link", b.Target())
}

// Mesh links every pair of ps.
func (c *Cluster) Mesh(ps ...*Proc) {
	for i := range ps {
		for _, q := range ps[i+1:] {
			c.Connect(ps[i], q)
		}
	}
}

// Cut stops every link between the two groups and refuses new ones.
func (c *Cluster) Cut(one, two []*Proc) {
	for _, a := range one {
		for _, b := range two {
			c.Do(a, "block", b.Addr)
			c.Do(b, "block", a.Addr)
		}
	}
}

// Heal lifts every block among ps and links them again.
func (c *Cluster) Heal(ps ...*Proc) {
	for _, a := range ps {
		for _, b := range ps {
			if a != b {
				c.Do(a, "unblock", b.Addr)
			}
		}
	}
	c.Mesh(ps...)
}

// Isolate cuts p from every other peer in ps.
func (c *Cluster) Isolate(p *Proc, ps []*Proc) {
	c.Cut([]*Proc{p}, slices.DeleteFunc(slices.Clone(ps), func(q *Proc) bool { return q == p }))
}

// Restart starts p again on its directory and port, and tells ps to drop their old links to it.
func (c *Cluster) Restart(p *Proc, ps []*Proc, extra ...string) *Proc {
	for _, q := range ps {
		if q != p && q.Alive() {
			c.Do(q, "unlink", p.Addr)
		}
	}
	n := c.Spawn(p.Name, append([]string{"--dir", p.Dir, "--listen", p.Addr}, extra...)...)
	c.attachTap(n)
	return n
}

// CopyDir copies p's data directory, stopped, into a directory of its own named name; p must be stopped.
func (c *Cluster) CopyDir(p *Proc, name string) string {
	dst := filepath.Join(c.root, name)
	c.Must(copyTree(p.Dir, dst))
	return dst
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		to := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(to, 0o700)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(to, b, 0o600)
	})
}

// Parallel runs f for every peer at once and fails the scenario with the first error.
func (c *Cluster) Parallel(ps []*Proc, f func(i int, p *Proc) error) {
	errs := make([]error, len(ps))
	var wg sync.WaitGroup
	for i, p := range ps {
		wg.Go(func() { errs[i] = f(i, p) })
	}
	wg.Wait()
	for _, e := range errs {
		c.Must(e)
	}
}

// Data returns the number of data entries p has settled.
func (c *Cluster) Data(p *Proc) uint64 {
	s, err := p.Status(c.ctx)
	c.Must(err)
	return s.SettledData
}

// Stat returns p's status.
func (c *Cluster) Stat(p *Proc) braidpeer.Status {
	s, err := p.Status(c.ctx)
	c.Must(err)
	return s
}

// Statuses asks every process for its status at once.
func (c *Cluster) Statuses(ps []*Proc) ([]braidpeer.Status, error) {
	out := make([]braidpeer.Status, len(ps))
	errs := make([]error, len(ps))
	var wg sync.WaitGroup
	for i, p := range ps {
		wg.Go(func() { out[i], errs[i] = p.Status(c.ctx) })
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return out, e
		}
	}
	return out, nil
}

// Until polls cond until it holds, and returns how long that took; it fails the scenario after limit.
func (c *Cluster) Until(what string, limit time.Duration, cond func() (bool, string)) time.Duration {
	t0 := time.Now()
	delay := 20 * time.Millisecond
	for {
		ok, detail := cond()
		if ok {
			return time.Since(t0)
		}
		if time.Since(t0) > limit || c.ctx.Err() != nil {
			c.Fatalf("%s: not reached in %v: %s", what, limit, detail)
		}
		select {
		case <-time.After(delay):
		case <-c.ctx.Done():
		}
		delay = min(delay*3/2, 250*time.Millisecond)
	}
}

// Summary describes statuses for a failure message.
func Summary(ps []*Proc, ss []braidpeer.Status) string {
	var b strings.Builder
	for i, s := range ss {
		name := "?"
		if i < len(ps) {
			name = ps[i].Name
		}
		fmt.Fprintf(&b, "\n  %s settled=%d data=%d/%d tail=%d order=%.8s content=%.8s seats=%d q=%d halted=%q sethalt=%q lost=%v unconf=%v links=%d members=%d",
			name, s.Settled, s.SettledData, s.TentativeData, s.Tail, s.Order, s.Content, s.Seats, s.Quorum, s.Halted, s.SettlementHalted,
			s.StrandLost, s.Unconfirmed, s.Links, len(s.Members))
	}
	return b.String()
}

// Converged is the condition that every peer of ps holds the same settled prefix and content, has settled all it holds,
// and has settled data data entries (0: not checked). It is meant for Until.
func (c *Cluster) Converged(ps []*Proc, data uint64) func() (bool, string) {
	return func() (bool, string) {
		ss, err := c.Statuses(ps)
		if err != nil {
			return false, err.Error()
		}
		ok := true
		for _, s := range ss {
			switch {
			case !s.HasBraid || s.Order == "" || s.OrderAt != s.Settled || s.Halted != "":
				ok = false
			case s.SettledData != s.TentativeData || (data > 0 && s.SettledData != data):
				ok = false
			case s.Order != ss[0].Order || s.Content != ss[0].Content || s.Settled != ss[0].Settled:
				ok = false
			}
		}
		return ok, Summary(ps, ss)
	}
}

// Wait fails the scenario unless ps converge on data settled data entries within limit, and records the time as name.
func (c *Cluster) Wait(name string, ps []*Proc, data uint64, limit time.Duration) time.Duration {
	d := c.Until(name, limit, c.Converged(ps, data))
	c.Time(name, d)
	return d
}

// Note records a fact the report should show.
func (c *Cluster) Note(format string, a ...any) {
	c.mu.Lock()
	c.notes = append(c.notes, fmt.Sprintf(format, a...))
	c.mu.Unlock()
	c.logf("  note "+format, a...)
}

// Time records a measured duration.
func (c *Cluster) Time(name string, d time.Duration) {
	c.mu.Lock()
	c.times = append(c.times, Timing{name, d})
	c.mu.Unlock()
	c.logf("  time %-28s %v", name, d.Round(time.Millisecond))
}

// Grow starts a founder and n-1 joiners with the default roles, links them, and waits until they agree on min(n, 4) seats.
func (c *Cluster) Grow(n int, prefix string) []*Proc {
	f := c.Founder(prefix + "0")
	ps := []*Proc{f}
	for i := 1; i < n; i++ {
		p := c.Join(f, fmt.Sprintf("%s%d", prefix, i))
		ps = append(ps, p)
		c.Mesh(ps...)
		c.Until(fmt.Sprintf("%s%d joined", prefix, i), 60*time.Second, func() (bool, string) {
			ss, err := c.Statuses(ps)
			if err != nil {
				return false, err.Error()
			}
			for _, s := range ss {
				if !slices.Contains(s.SettledMembers, p.Key) || s.Halted != "" {
					return false, Summary(ps, ss)
				}
			}
			return true, ""
		})
	}
	c.GrowSeats(ps, min(n, 4))
	return ps
}

// GrowSeats writes until every peer reports a settler config of want seats; a config activates after settled entries.
func (c *Cluster) GrowSeats(ps []*Proc, want int) {
	c.Until("seats", 90*time.Second, func() (bool, string) {
		ss, err := c.Statuses(ps)
		if err != nil {
			return false, err.Error()
		}
		done := true
		for _, s := range ss {
			done = done && s.Seats == want && len(s.Pending) == 0
		}
		if !done {
			c.Do(ps[0], "put", "grow", "1")
		}
		return done, Summary(ps, ss)
	})
	c.Until("seats agree", 60*time.Second, c.Converged(ps, 0))
}

// DHT starts an in-process kolo-dht entry node on 127.0.0.1 and returns its address.
func (c *Cluster) DHT() netip.AddrPort {
	id, err := koloid.Generate()
	c.Must(err)
	n, err := kolodht.New(c.ctx, "127.0.0.1:0", id, &kolodht.Config{Server: true})
	c.Must(err)
	c.dht = n
	return n.Addr().(*net.UDPAddr).AddrPort()
}

func unmarshal(d []byte, v any) error { return json.Unmarshal(d, v) }
