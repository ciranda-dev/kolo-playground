// Command kolo-peer joins a Kolo topic through kolo-dht bootstrap nodes, connects to its peers and pings them.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	koloid "github.com/ciranda-dev/kolo-id"
	kolorelay "github.com/ciranda-dev/kolo-relay"
	koloswarm "github.com/ciranda-dev/kolo-swarm"
)

const (
	topicContext = "kolo-playground/topic/v0"
	maxTopic     = 256
	maxBootstrap = 8
)

// shutdownTimeout and relayDialTimeout are vars so that a test can shorten them.
var (
	shutdownTimeout = 10 * time.Second
	// relayDialTimeout caps a relay dial and its hello, which hold a slot. kolo-relay's own bounds add up to about 60s,
	// so 30s frees a stalled slot sooner and the next unreachable event tries again.
	relayDialTimeout = 30 * time.Second
)

type options struct {
	bootstrap     []netip.AddrPort
	topic         string
	listen        netip.AddrPort
	key           string
	relay         netip.AddrPort
	relayKey      ed25519.PublicKey
	serveRelay    bool
	allowLoopback bool
	maxPeers      int
	verbose       bool
}

// v formats only with --verbose, so addresses and full keys stay out of the default log.
func (o options) v(format string, a ...any) string {
	if !o.verbose {
		return ""
	}
	return fmt.Sprintf(format, a...)
}

// at returns " at IP:PORT" with --verbose; a relayed peer has no address.
func (o options) at(ap netip.AddrPort) string {
	if !ap.IsValid() {
		return ""
	}
	return o.v(" at %s", ap)
}

// ipv4 parses IP:PORT, or also :PORT and port 0 for a listen address. The Kolo libraries are IPv4 only.
func ipv4(s string, listen bool) (netip.AddrPort, error) {
	if listen && strings.HasPrefix(s, ":") {
		s = "0.0.0.0" + s
	}
	ap, err := netip.ParseAddrPort(s)
	switch {
	case err != nil || ap.Port() == 0 && !listen:
		return ap, errors.New("want IP:PORT")
	case !ap.Addr().Is4():
		return ap, errors.New("IPv6 input is refused; want an IPv4 IP:PORT")
	}
	return ap, nil
}

func main() {
	sigs := []os.Signal{os.Interrupt, syscall.SIGTERM}
	if !signal.Ignored(syscall.SIGHUP) { // as under nohup
		sigs = append(sigs, syscall.SIGHUP)
	}
	ctx, stop := signal.NotifyContext(context.Background(), sigs...)
	context.AfterFunc(ctx, stop) // a second signal then ends the process at once
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// parseFlags returns the options, flag.ErrHelp, or an error it already printed with the usage.
func parseFlags(args []string, stderr io.Writer) (options, error) {
	o := options{listen: netip.MustParseAddrPort("0.0.0.0:0")}
	fset := flag.NewFlagSet("kolo-peer", flag.ContinueOnError)
	fset.SetOutput(stderr)
	fset.Func("bootstrap", "`IP:PORT` of a kolo-dht bootstrap node such as kolo-node (repeatable, at most 8)", func(s string) error {
		ap, err := ipv4(s, false)
		o.bootstrap = append(o.bootstrap, ap)
		return err
	})
	fset.StringVar(&o.topic, "topic", "", "topic `name` to join (required)")
	fset.Func("listen", "UDP `address` to listen on, IP:PORT or :PORT (default :0)", func(s string) (err error) {
		o.listen, err = ipv4(s, true)
		return err
	})
	fset.StringVar(&o.key, "key", "", "identity `file`, created if missing; empty: a new identity in a temporary directory removed on exit")
	fset.Func("relay", "kolo-relay server at `IP:PORT@KEY`, KEY its public key in hex: reserve there, and dial through it the peers kolo-dht cannot reach", func(s string) (err error) {
		addr, key, _ := strings.Cut(s, "@")
		if o.relay, err = ipv4(addr, false); err != nil {
			return err
		}
		if o.relayKey, err = hex.DecodeString(key); err != nil || len(o.relayKey) != ed25519.PublicKeySize {
			return errors.New("want IP:PORT@KEY, KEY the relay's public key in 64 hex digits")
		}
		return nil
	})
	fset.BoolVar(&o.serveRelay, "serve-relay", false, "run a kolo-relay server on --listen instead of a peer")
	fset.BoolVar(&o.allowLoopback, "allow-loopback", false, "allow loopback bootstrap nodes and peers, to run peers on one machine")
	fset.IntVar(&o.maxPeers, "max-peers", 4, "most `peers` connected or being dialed, 1 to 32")
	fset.BoolVar(&o.verbose, "verbose", false, "log addresses, full keys and the topic hash")
	if err := fset.Parse(args); err != nil {
		return o, err
	}
	peerFlag, listen := false, false
	fset.Visit(func(f *flag.Flag) {
		peerFlag = peerFlag || !strings.Contains(" serve-relay listen key verbose ", " "+f.Name+" ")
		listen = listen || f.Name == "listen"
	})
	var err error
	switch {
	case fset.NArg() > 0:
		err = errors.New("unexpected argument")
	case o.serveRelay && peerFlag:
		err = errors.New("--serve-relay takes only --listen, --key and --verbose")
	case o.serveRelay && !listen:
		err = errors.New("--serve-relay needs --listen")
	case o.serveRelay:
	case len(o.bootstrap) == 0:
		err = errors.New("--bootstrap is required")
	case len(o.bootstrap) > maxBootstrap:
		err = fmt.Errorf("at most %d --bootstrap", maxBootstrap)
	case o.topic == "":
		err = errors.New("--topic is required")
	case len(o.topic) > maxTopic || !utf8.ValidString(o.topic):
		err = fmt.Errorf("--topic must be at most %d bytes of UTF-8", maxTopic)
	case o.maxPeers < 1 || o.maxPeers > 32:
		err = errors.New("--max-peers must be between 1 and 32")
	}
	loopback := func(name string, ap netip.AddrPort) {
		if err == nil && ap.Addr().IsLoopback() && !o.allowLoopback {
			err = fmt.Errorf("%s %s is a loopback address; add --allow-loopback to run peers on one machine", name, ap)
		}
	}
	for _, ap := range o.bootstrap {
		loopback("--bootstrap", ap)
	}
	loopback("--relay", o.relay)
	if err != nil {
		fmt.Fprintf(stderr, "kolo-peer: %v\n", err)
		fset.Usage()
	}
	return o, err
}

func topicOf(name string) koloswarm.Topic {
	return koloswarm.Topic(sha256.Sum256([]byte(topicContext + name)))
}

func short(key []byte) string {
	return hex.EncodeToString(key[:min(len(key), 4)])
}

func allow(loopback bool) func(ed25519.PublicKey, netip.AddrPort) bool {
	return func(_ ed25519.PublicKey, ap netip.AddrPort) bool {
		return loopback || !ap.Addr().IsLoopback()
	}
}

// identity with an empty path saves a new key in a temporary directory that cleanup removes.
func identity(path string) (id *koloid.Identity, cleanup func(), err error) {
	cleanup = func() {}
	if path == "" {
		dir, err := os.MkdirTemp("", "kolo-peer-")
		if err != nil {
			return nil, nil, err
		}
		cleanup = func() { os.RemoveAll(dir) }
		path = filepath.Join(dir, "peer.key")
	} else if id, err = koloid.Load(path); !errors.Is(err, fs.ErrNotExist) {
		return id, cleanup, err
	}
	if id, err = koloid.Generate(); err == nil {
		err = id.SaveNew(path)
	}
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return id, cleanup, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	o, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	} else if err != nil {
		return 2
	}
	fail := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "kolo-peer: "+format+"\n", a...)
		return 1
	}
	id, cleanup, err := identity(o.key)
	if err != nil {
		return fail("identity: %v", err)
	}
	defer cleanup()
	lg := log.New(stdout, "", log.Ltime)
	logID := func(addr net.Addr) {
		lg.Printf("id %s%s", short(id.PublicKey()), o.v(" key %x listen %s", []byte(id.PublicKey()), addr))
	}
	var c io.Closer
	var wg sync.WaitGroup
	if o.serveRelay {
		var r *kolorelay.Server
		if r, err = kolorelay.Listen(ctx, o.listen.String(), id, nil); err != nil {
			return fail("listen: %v", err)
		}
		c = r
		logID(r.Addr())
		lg.Printf("relay ready key %x listen %s", []byte(id.PublicKey()), r.Addr())
		<-ctx.Done()
	} else {
		var s *koloswarm.Swarm
		if s, err = koloswarm.New(ctx, o.listen.String(), id, &koloswarm.Config{MaxPeers: o.maxPeers, Allow: allow(o.allowLoopback)}); err != nil {
			return fail("listen: %v", err)
		}
		c = s
		logID(s.Addr())
		err = serve(ctx, s, id, o, lg, &wg)
	}
	if ctx.Err() != nil {
		cleanup() // now, since a second signal ends the process at once
		lg.Print("stopping")
	}
	done := make(chan struct{})
	go func() {
		c.Close()
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		return fail("shutdown timed out")
	}
	if ctx.Err() == nil {
		return fail("%v", err)
	}
	lg.Print("stopped")
	return 0
}

func serve(ctx context.Context, s *koloswarm.Swarm, me *koloid.Identity, o options, lg *log.Logger, wg *sync.WaitGroup) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // stops the reservation and the relay dials when serve fails
	if err := s.Bootstrap(ctx, o.bootstrap); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	lg.Printf("bootstrap reached%s", o.v(" %v", o.bootstrap))
	if o.relay.IsValid() {
		res, err := kolorelay.Reserve(ctx, o.relay.String(), o.relayKey, me, nil)
		if err != nil {
			return fmt.Errorf("relay: %w", err)
		}
		lg.Printf("relay reserved%s", o.at(o.relay))
		wg.Add(1)
		go func() {
			defer wg.Done()
			accept(ctx, s, res, lg, wg)
		}()
	}
	topic := topicOf(o.topic)
	if err := s.Join(ctx, topic, nil); err != nil {
		return fmt.Errorf("join: %w", err)
	}
	lg.Printf("joined topic %q%s", o.topic, o.v(" %x", topic[:]))
	// dialing holds the relay dials in progress: at most MaxPeers, one per key, as Unreachable repeats.
	var mu sync.Mutex
	dialing := map[string]bool{}
	for {
		ev, err := s.Next(ctx)
		if err != nil {
			return err
		}
		id := short(ev.PublicKey)
		switch ev.Kind {
		case koloswarm.Found:
			lg.Printf("peer found %s%s", id, o.at(ev.Addr))
		case koloswarm.Connected:
			p := ev.Peer
			path := "direct"
			if !p.Direct() {
				path = "relayed"
			}
			lg.Printf("peer connected %s %s%s", short(p.PublicKey()), path, o.at(p.Addr()))
			wg.Add(2)
			go func() {
				defer wg.Done()
				pinger(ctx, p, lg)
			}()
			go func() {
				defer wg.Done()
				respond(ctx, p.Conn(), pingTimeout)
			}()
		case koloswarm.Unreachable:
			if !o.relay.IsValid() {
				lg.Printf("peer unreachable %s%s: hole punching failed, no relay, retrying", id, o.at(ev.Addr))
				break
			}
			key := string(ev.PublicKey)
			mu.Lock()
			again, full := dialing[key], len(dialing) >= o.maxPeers
			if !again && !full {
				dialing[key] = true
			}
			mu.Unlock()
			if full && !again {
				lg.Printf("peer unreachable %s%s: hole punching failed, relay busy, retrying", id, o.at(ev.Addr))
			}
			if again || full {
				break
			}
			lg.Printf("peer unreachable %s%s: hole punching failed, trying the relay", id, o.at(ev.Addr))
			wg.Add(1)
			go func() {
				defer wg.Done()
				dctx, cancel := context.WithTimeout(ctx, relayDialTimeout)
				defer cancel()
				c, err := kolorelay.Dial(dctx, o.relay.String(), o.relayKey, me, ev.PublicKey, nil)
				if err == nil {
					err = s.Attach(dctx, c, true)
				}
				mu.Lock()
				delete(dialing, key)
				mu.Unlock()
				if err != nil && ctx.Err() == nil {
					lg.Printf("relay %s failed: %v", id, err)
				}
			}()
		case koloswarm.Lost:
			lg.Printf("peer lost %s%s: %v", id, o.at(ev.Addr), ev.Err)
		}
	}
}

// accept keeps res open after the relay ends it, since closing it would close the peers it brought.
func accept(ctx context.Context, s *koloswarm.Swarm, res *kolorelay.Reservation, lg *log.Logger, wg *sync.WaitGroup) {
	defer res.Close()
	for {
		c, err := res.Accept(ctx)
		if err != nil {
			if ctx.Err() == nil {
				lg.Printf("relay reservation ended: %v", err)
				<-ctx.Done()
			}
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Attach(ctx, c, false)
		}()
	}
}
