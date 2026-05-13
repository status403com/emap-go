package emap

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// dialBareConn dials the fake server and runs the LOGIN/CAPABILITY/SELECT
// bring-up directly, returning a ready-to-IDLE imapConn. Bypasses the
// session/Manager so wire-level tests don't contend with the session-level
// IDLE loop for the conn's execMu.
func dialBareConn(t *testing.T, srv *fakeServer) *imapConn {
	t.Helper()
	c, err := srv.dialer()("imap.example.com", 993, true)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := c.login("alice@example.com", "pw"); err != nil {
		c.Close()
		t.Fatalf("login: %v", err)
	}
	if err := c.capability(); err != nil {
		c.Close()
		t.Fatalf("capability: %v", err)
	}
	if err := c.selectFolder("INBOX"); err != nil {
		c.Close()
		t.Fatalf("select: %v", err)
	}
	return c
}

func TestIdleReturnsCleanlyOnCtxCancel(t *testing.T) {
	srv := newFakeServer()
	srv.script = idleNoEventsScript
	c := dialBareConn(t, srv)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	if err := c.idle(ctx, nil); err != nil {
		t.Fatalf("idle returned %v, expected nil", err)
	}
}

func TestIdleDispatchesUntaggedResponses(t *testing.T) {
	srv := newFakeServer()
	srv.script = idleWithEventsScript
	c := dialBareConn(t, srv)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(60 * time.Millisecond)
		cancel()
	}()

	var mu sync.Mutex
	var events []string
	err := c.idle(ctx, func(line string) {
		mu.Lock()
		events = append(events, line)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("idle: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d: %v", len(events), events)
	}
	for i, want := range []string{"5 EXISTS", "6 EXISTS"} {
		if events[i] != want {
			t.Fatalf("event %d = %q, want %q", i, events[i], want)
		}
	}
}

func TestIdleErrorsOnBadContinuation(t *testing.T) {
	srv := newFakeServer()
	srv.script = idleBadContinuationScript
	c := dialBareConn(t, srv)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.idle(ctx, nil)
	if err == nil {
		t.Fatal("expected error when server rejects IDLE with NO")
	}
	if !strings.Contains(err.Error(), "expected `+ idling`") && !strings.Contains(err.Error(), "IDLE terminated") {
		t.Fatalf("unexpected error shape: %v", err)
	}
}

func TestIdleErrorsOnConnectionDrop(t *testing.T) {
	srv := newFakeServer()
	srv.script = idleNoEventsScript
	c := dialBareConn(t, srv)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(40 * time.Millisecond)
		srv.dropConn()
	}()

	err := c.idle(ctx, nil)
	if err == nil {
		t.Fatal("expected error when conn drops during IDLE")
	}
}
