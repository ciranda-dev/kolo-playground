package main

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// Costs are the measured costs of a scenario.
type Costs struct {
	Peers                        int
	Tap                          TapStats
	UDPOutBytes, UDPInBytes      int64
	ReplicaCommits, ReplicaBytes int64
	Admit, JoinTrip              []time.Duration
}

// Report is the outcome of one scenario.
type Report struct {
	Name     string
	Err      error
	Duration time.Duration
	Timings  []Timing
	Notes    []string
	Costs    Costs
}

// costs refreshes the status of every live process and adds up what each reported, those that have been stopped included.
func (c *Cluster) costs() Costs {
	c.mu.Lock()
	all := append([]*Proc(nil), c.all...)
	c.mu.Unlock()
	var out Costs
	for _, p := range all {
		if p.Alive() {
			p.Status(c.ctx)
		}
		s := p.Last()
		out.Peers++
		out.UDPOutBytes += s.UDPOutBytes
		out.UDPInBytes += s.UDPInBytes
		out.ReplicaCommits += s.ReplicaCommits
		out.ReplicaBytes += s.ReplicaBytes
	}
	if c.tap != nil {
		out.Tap = c.tap.Stats()
	}
	c.mu.Lock()
	out.Admit = append(out.Admit, c.admit...)
	out.JoinTrip = append(out.JoinTrip, c.joinTrip...)
	c.mu.Unlock()
	return out
}

func durations(ds []time.Duration) string {
	var parts []string
	for _, d := range ds {
		parts = append(parts, d.Round(time.Millisecond).String())
	}
	return strings.Join(parts, " ")
}

func kib(n int64) string { return fmt.Sprintf("%.1f KiB", float64(n)/1024) }

// Print writes the report as plain text.
func (r *Report) Print(w io.Writer) {
	status := "ok"
	if r.Err != nil {
		status = "FAILED"
	}
	fmt.Fprintf(w, "scenario %s: %s in %v\n", r.Name, status, r.Duration.Round(100*time.Millisecond))
	for _, t := range r.Timings {
		fmt.Fprintf(w, "  time    %-30s %v\n", t.Name, t.D.Round(time.Millisecond))
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "  note    %s\n", n)
	}
	c := r.Costs
	perLink := int64(0)
	if c.Tap.Links > 0 {
		perLink = int64(c.Tap.AnnounceBytes / c.Tap.Links)
	}
	fmt.Fprintf(w, "  cost    announce %d msgs %s over %d tap links (%s per link); evidence %d msgs %s; %d requests to the tap\n",
		c.Tap.AnnounceMsgs, kib(int64(c.Tap.AnnounceBytes)), c.Tap.Links, kib(perLink), c.Tap.EvidenceMsgs,
		kib(int64(c.Tap.EvidenceBytes)), c.Tap.RequestMsgs)
	fmt.Fprintf(w, "  cost    pulls: %d replica commits (%s) over %d processes; udp out %s in %s\n", c.ReplicaCommits,
		kib(c.ReplicaBytes), c.Peers, kib(c.UDPOutBytes), kib(c.UDPInBytes))
	if len(c.Admit) > 0 {
		fmt.Fprintf(w, "  cost    admission: Admit call %s; Admit to Complete round trip %s\n", durations(c.Admit), durations(c.JoinTrip))
	}
	if r.Err != nil {
		fmt.Fprintf(w, "  error   %v\n", r.Err)
	}
}
