package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log"
	"os"
	"time"

	koloswarm "github.com/ciranda-dev/kolo-swarm"
	kolotransport "github.com/ciranda-dev/kolo-transport"
)

const (
	pingInterval = 5 * time.Second
	pingTimeout  = 4 * time.Second
	pingSize     = 9 // version 0 (kolo-codec uvarint) and an 8-byte nonce
	codeBadPing  = 1
)

func decodePing(b []byte) bool {
	return len(b) == pingSize && b[0] == 0
}

func pinger(ctx context.Context, p *koloswarm.Peer, lg *log.Logger) {
	conn := p.Conn()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(conn.Context(), cancel)()
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rtt, err := ping(ctx, conn, pingTimeout)
		switch {
		case ctx.Err() != nil || conn.Context().Err() != nil:
			return
		case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
			lg.Printf("ping %s failed: timeout", short(p.PublicKey()))
		case err != nil:
			lg.Printf("ping %s failed: %v", short(p.PublicKey()), err)
		default:
			lg.Printf("ping %s rtt %.1fms", short(p.PublicKey()), float64(rtt)/float64(time.Millisecond))
		}
	}
}

// ping's round trip time runs from the write of the ping to the end of the echo.
func ping(ctx context.Context, c koloswarm.Conn, timeout time.Duration) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req := make([]byte, pingSize)
	rand.Read(req[1:])
	st, err := c.OpenStream(ctx)
	if err != nil {
		return 0, err
	}
	defer context.AfterFunc(ctx, func() { st.SetDeadline(time.Now()) })()
	start := time.Now()
	_, err = st.Write(req)
	st.Close()
	var reply []byte
	if err == nil {
		reply, err = io.ReadAll(io.LimitReader(st, pingSize+1))
	}
	rtt := time.Since(start)
	if err == nil && !bytes.Equal(reply, req) {
		err = errors.New("bad reply")
	}
	if err != nil {
		st.CancelRead(codeBadPing)
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, err
	}
	return rtt, nil
}

// respond serves one stream at a time, so a peer cannot make it run many echoes at once.
func respond(ctx context.Context, c koloswarm.Conn, timeout time.Duration) {
	for {
		st, err := c.AcceptStream(ctx)
		if err != nil {
			return
		}
		echo(ctx, st, timeout)
	}
}

func echo(ctx context.Context, st *kolotransport.Stream, timeout time.Duration) {
	st.SetDeadline(time.Now().Add(timeout))
	defer context.AfterFunc(ctx, func() { st.SetDeadline(time.Now()) })()
	b, err := io.ReadAll(io.LimitReader(st, pingSize+1))
	if err == nil && decodePing(b) {
		if _, err = st.Write(b); err == nil {
			st.Close()
			return
		}
	}
	st.CancelRead(codeBadPing)
	st.CancelWrite(codeBadPing)
}
