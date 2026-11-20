package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ciranda-dev/kolo-playground/internal/braidpeer"
)

// peerArg as the first argument makes this binary run as a braid-peer; peerBin, if set, is a braid-peer binary to run instead.
const peerArg = "__peer"

var peerBin string

// Proc is one braid-peer process.
type Proc struct {
	Name string
	Dir  string
	Key  string
	Addr string
	// Request is the join request of a process started with --join.
	Request string
	Args    []string

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	exited chan struct{}

	mu      sync.Mutex
	nextID  int
	waiting map[string]chan reply
	events  []braidEvent
	last    braidpeer.Status
	errTail bytes.Buffer
	killed  bool
}

type reply struct {
	OK   bool            `json:"ok"`
	Err  string          `json:"err"`
	Data json.RawMessage `json:"data"`
}

type braidEvent struct {
	Kind     string `json:"kind"`
	Position uint64 `json:"position"`
	Member   string `json:"member"`
	Err      string `json:"err"`
}

// startProc starts a braid-peer and waits for its ready line.
func startProc(ctx context.Context, name string, args []string) (*Proc, error) {
	exe, lead := peerBin, []string{}
	if exe == "" {
		var err error
		if exe, err = os.Executable(); err != nil {
			return nil, err
		}
		lead = []string{peerArg}
	}
	p := &Proc{Name: name, Args: args, exited: make(chan struct{}), waiting: map[string]chan reply{}}
	p.cmd = exec.Command(exe, append(append(lead, "--name", name), args...)...)
	setGroup(p.cmd)
	p.cmd.Stderr = &lockedWriter{p: p}
	out, err := p.cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if p.stdin, err = p.cmd.StdinPipe(); err != nil {
		return nil, err
	}
	if err := p.cmd.Start(); err != nil {
		return nil, err
	}
	ready := make(chan error, 1)
	go p.read(out, ready)
	go func() {
		p.cmd.Wait()
		close(p.exited)
	}()
	select {
	case err := <-ready:
		if err != nil {
			p.Kill()
			return nil, fmt.Errorf("%s: %w\n%s", name, err, p.Stderr())
		}
	case <-ctx.Done():
		p.Kill()
		return nil, ctx.Err()
	case <-time.After(60 * time.Second):
		p.Kill()
		return nil, fmt.Errorf("%s: no ready line in 60s\n%s", name, p.Stderr())
	}
	return p, nil
}

type lockedWriter struct{ p *Proc }

func (w *lockedWriter) Write(b []byte) (int, error) {
	w.p.mu.Lock()
	defer w.p.mu.Unlock()
	if w.p.errTail.Len() > 16<<10 {
		w.p.errTail.Reset()
	}
	return w.p.errTail.Write(b)
}

// Stderr returns the tail of the process's stderr.
func (p *Proc) Stderr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.errTail.String()
}

func (p *Proc) read(out io.Reader, ready chan<- error) {
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 0, 64<<10), 64<<20)
	got := false
	for sc.Scan() {
		var m struct {
			Ev      string `json:"ev"`
			ID      string `json:"id"`
			Key     string `json:"key"`
			Addr    string `json:"addr"`
			Dir     string `json:"dir"`
			Request string `json:"request"`
		}
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			continue
		}
		switch m.Ev {
		case "ready":
			p.Key, p.Addr, p.Dir, p.Request = m.Key, m.Addr, m.Dir, m.Request
			got = true
			ready <- nil
		case "braid":
			p.mu.Lock()
			var ev braidEvent
			json.Unmarshal(sc.Bytes(), &ev)
			p.events = append(p.events, ev)
			p.mu.Unlock()
		case "reply":
			p.mu.Lock()
			ch := p.waiting[m.ID]
			delete(p.waiting, m.ID)
			p.mu.Unlock()
			if ch != nil {
				var r reply
				json.Unmarshal(sc.Bytes(), &r)
				ch <- r
			}
		}
	}
	if !got {
		ready <- errors.New("exited before ready")
	}
	p.mu.Lock()
	for id, ch := range p.waiting {
		ch <- reply{Err: "process exited"}
		delete(p.waiting, id)
	}
	p.mu.Unlock()
}

// Do sends a command and waits for its reply.
func (p *Proc) Do(ctx context.Context, cmd string, args ...string) (json.RawMessage, error) {
	p.mu.Lock()
	p.nextID++
	id := strconv.Itoa(p.nextID)
	ch := make(chan reply, 1)
	p.waiting[id] = ch
	p.mu.Unlock()
	line := id + " " + cmd
	if len(args) > 0 {
		line += " " + strings.Join(args, " ")
	}
	if _, err := io.WriteString(p.stdin, line+"\n"); err != nil {
		return nil, fmt.Errorf("%s %s: %w", p.Name, cmd, err)
	}
	select {
	case r := <-ch:
		if r.Err != "" {
			return r.Data, fmt.Errorf("%s %s: %s", p.Name, cmd, r.Err)
		}
		return r.Data, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%s %s: %w", p.Name, cmd, ctx.Err())
	case <-p.exited:
		return nil, fmt.Errorf("%s %s: process exited\n%s", p.Name, cmd, p.Stderr())
	}
}

// Status asks for the peer's status and remembers it.
func (p *Proc) Status(ctx context.Context) (braidpeer.Status, error) {
	d, err := p.Do(ctx, "status")
	if err != nil {
		return braidpeer.Status{}, err
	}
	var s braidpeer.Status
	if err := json.Unmarshal(d, &s); err != nil {
		return s, err
	}
	p.mu.Lock()
	p.last = s
	p.mu.Unlock()
	return s, nil
}

// Last returns the last status Status saw.
func (p *Proc) Last() braidpeer.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last
}

// Events counts the notable braid events the process printed.
func (p *Proc) Events(kind string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// EventErrs returns the error text of every notable event of a kind.
func (p *Proc) EventErrs(kind string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, e := range p.events {
		if e.Kind == kind {
			out = append(out, fmt.Sprintf("pos %d: %s", e.Position, e.Err))
		}
	}
	return out
}

// Alive reports whether the process runs.
func (p *Proc) Alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

// Kill ends the process and its group at once, as kill -9 does.
func (p *Proc) Kill() {
	p.mu.Lock()
	p.killed = true
	p.mu.Unlock()
	if p.cmd.Process != nil {
		killGroup(p.cmd.Process)
	}
	if p.exited != nil {
		<-p.exited
	}
}

// Stop asks the process to quit and kills it if it does not exit in time.
func (p *Proc) Stop(ctx context.Context) {
	if !p.Alive() {
		return
	}
	qctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	p.Do(qctx, "quit")
	p.stdin.Close()
	select {
	case <-p.exited:
	case <-time.After(20 * time.Second):
		p.Kill()
	}
}
