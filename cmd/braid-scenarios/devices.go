package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"
)

// Restore replaces p's data directory, stopped, with the files of dir, as a restored backup does.
func (c *Cluster) Restore(p *Proc, dir string) {
	c.Must(os.RemoveAll(p.Dir))
	c.Must(copyTree(dir, p.Dir))
}

// Relink dials from p to every peer of ps and from the tap side as Restart does.
func (c *Cluster) Relink(p *Proc, ps []*Proc) {
	for _, q := range ps {
		if q != p && q.Alive() {
			c.Connect(p, q)
		}
	}
}

// restoreBehind: a settler restored from an old copy is only behind its own strand, so it catches up and writes again.
func restoreBehind(c *Cluster) {
	ps := c.Grow(4, "r")
	s := ps[3]
	c.Do(s, "putn", "pre", "10")
	c.Wait("settle before the backup", ps, 0, wait)
	s.Stop(c.ctx)
	backup := c.CopyDir(s, "backup")
	s = c.Restart(s, ps)
	ps[3] = s
	c.Relink(s, ps)
	base := c.Data(ps[0])
	c.Until("settler confirmed after the first reopen", wait, func() (bool, string) { return !c.Stat(s).Unconfirmed, "" })
	c.Do(s, "putn", "post", "20")
	c.Wait("settle the entries written after the backup", ps, base+20, wait)
	s.Stop(c.ctx)
	c.Restore(s, backup)
	s = c.Restart(s, ps)
	ps[3] = s
	c.Relink(s, ps)
	d := c.Until("restored settler catches up", wait, func() (bool, string) {
		st := c.Stat(s)
		return st.Events["strandCaughtUp"] >= 1 && !st.Unconfirmed && !st.StrandLost, fmt.Sprintf("%+v", st.Events)
	})
	c.Time("catch up the own strand", d)
	c.Do(s, "putn", "again", "10")
	c.Wait("settle after the catch-up", ps, base+30, wait)
	c.Note("restored from an old copy, the settler was only behind: it caught up, wrote 10 entries and all agree")
}

// restoreInconsistent: a copy that wrote offline conflicts with what the others hold: writes are refused, and the member
// recovers by joining again with a new strand.
func restoreInconsistent(c *Cluster) {
	ps := c.Grow(5, "i")
	seats, w := ps[:4], ps[4]
	c.Do(w, "putn", "pre", "10")
	c.Wait("settle before the backup", ps, 0, wait)
	w.Stop(c.ctx)
	backup := c.CopyDir(w, "backup")
	w = c.Restart(w, ps)
	ps[4] = w
	c.Relink(w, ps)
	c.Until("confirmed", wait, func() (bool, string) { return !c.Stat(w).Unconfirmed, "" })
	c.Do(w, "putn", "x", "10")
	c.Wait("settle the entries written after the backup", ps, 0, wait)
	w.Stop(c.ctx)
	offline := c.Clone("w-offline", backup)
	c.Do(offline, "putn", "y", "5")
	offline.Stop(c.ctx)
	idPath := filepath.Join(backup, "identity")
	restored := c.Spawn("w", "--dir", backup, "--listen", w.Addr)
	c.attachTap(restored)
	c.Relink(restored, seats)
	d := c.Until("restored copy learns its strand is lost", wait, func() (bool, string) {
		st := c.Stat(restored)
		return st.StrandLost, fmt.Sprintf("%+v", st.Events)
	})
	c.Time("detect the inconsistent copy", d)
	err := c.DoErr(restored, "put", "refused", "1")
	c.Check(err != nil, "a lost strand accepted a write")
	c.Note("writes refused on the inconsistent copy: %v", err)
	restored.Stop(c.ctx)
	c.Until("seats agree", wait, c.Converged(seats, 0))
	roster := mustStatuses(c, seats[:1])[0].SettledMembers
	opts := []string{}
	if slices.Contains(roster, w.Key) {
		opts = append(opts, "replace")
	}
	fresh := c.JoinWith(seats[0], "w-new", []string{"--identity", idPath}, opts...)
	c.Mesh(append(slices.Clone(seats), fresh)...)
	live := append(slices.Clone(seats), fresh)
	c.Until("recovered member joined", wait, c.Converged(live, 0))
	c.Do(fresh, "putn", "recovered", "5")
	n := c.Data(seats[0])
	c.Wait("settle the new strand's writes", live, n+5, wait)
	c.Note("recovery through a new strand: the member joined again with its key (admit options %v) and wrote 5 entries", opts)
}

// kill9: a peer is killed up to three times while settling a long run, restarted on its data directory each time, and
// agrees with the others in the end.
func kill9(c *Cluster) {
	ps := c.Grow(3, "x")
	victim, writer := ps[1], ps[0]
	base := c.Data(writer)
	const n = 2000
	var finished atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- c.DoErr(writer, "putn", "big", fmt.Sprint(n))
		finished.Store(true)
	}()
	kills, last := 0, uint64(0)
	for kills < 3 && !finished.Load() {
		var settled, held uint64
		c.Until(fmt.Sprintf("victim settles %d more entries", 200), wait, func() (bool, string) {
			s, err := victim.Status(c.ctx)
			if err != nil {
				return false, err.Error()
			}
			settled, held = s.SettledData-base, s.TentativeData-base
			return finished.Load() || settled > last+200 && settled+20 < n, fmt.Sprintf("settled %d held %d of %d", settled, held, n)
		})
		if finished.Load() && settled+20 >= n {
			break
		}
		victim.Kill()
		victim = c.Restart(victim, ps)
		ps[1] = victim
		c.Relink(victim, ps)
		kills++
		last = settled
		c.Note("kill %d: %d of %d entries settled and %d held when the victim died", kills, settled, n, held)
	}
	c.Check(kills > 0, "the writer finished before the victim could be killed")
	c.Must(<-done)
	d := c.Wait("settle after the kills and restarts", ps, base+n, wait)
	c.Note("after the last restart all agree in %v", d.Round(time.Millisecond))
	for _, s := range mustStatuses(c, ps) {
		c.Check(s.Events["diverged"] == 0 && s.Halted == "", "%s diverged or halted after the restarts: %v %q", s.Name, s.Events, s.Halted)
	}
}

type admitReply struct {
	Welcome string `json:"welcome"`
	Nanos   int64  `json:"nanos"`
}

func admitReq(c *Cluster, inviter, joiner *Proc, opts ...string) admitReply {
	var r admitReply
	c.Must(json.Unmarshal(c.Do(inviter, "admit", append([]string{joiner.Request}, opts...)...), &r))
	return r
}
