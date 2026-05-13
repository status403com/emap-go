package emap

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReconnectsAfterConnDropDuringIdle(t *testing.T) {
	prevRound := IdleRoundDuration
	IdleRoundDuration = 30 * time.Second
	defer func() { IdleRoundDuration = prevRound }()

	prevBackoff := ReconnectInitialBackoff
	ReconnectInitialBackoff = 10 * time.Millisecond
	defer func() { ReconnectInitialBackoff = prevBackoff }()

	srv := newFakeServer()
	var dialCount atomic.Int32
	srv.script = func(fc *fakeConn) {
		n := dialCount.Add(1)
		handleIdleBringUpWithUIDNext(fc, 100)
		if n >= 2 {
			// After reconnect the session loop immediately issues a probe
			// FETCH (in case mail arrived during the outage). Deliver the
			// verification mail there — that's the realistic recovery flow.
			handleFetchCommand(fc, []fakeMail{{
				UID:     100,
				To:      "alice@example.com",
				From:    "no-reply@vendor.com",
				Subject: "Verify",
				Body:    "post-reconnect verification body",
			}})
		}
		for {
			if !handleIdleRound(fc, nil) {
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{To: "alice@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Wait for the initial IDLE to be observed by the server.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && srv.idleStarts.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.idleStarts.Load() < 1 {
		t.Fatal("initial IDLE never reached the server")
	}

	srv.dropConn() // simulates network blip / server BYE

	select {
	case msg, ok := <-sub.Ch:
		if !ok {
			t.Fatal("subscription channel closed unexpectedly")
		}
		if msg.UID != 100 {
			t.Fatalf("UID = %d, want 100", msg.UID)
		}
		if !strings.Contains(msg.Body, "post-reconnect") {
			t.Fatalf("body = %q, want post-reconnect payload", msg.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never received mail after reconnect")
	}

	if got := dialCount.Load(); got < 2 {
		t.Errorf("expected ≥2 dials (initial + reconnect), got %d", got)
	}
}

func TestReconnectBackoffRetriesUntilSuccess(t *testing.T) {
	prevInit := ReconnectInitialBackoff
	prevMax := ReconnectMaxBackoff
	ReconnectInitialBackoff = 10 * time.Millisecond
	ReconnectMaxBackoff = 50 * time.Millisecond
	defer func() {
		ReconnectInitialBackoff = prevInit
		ReconnectMaxBackoff = prevMax
	}()

	srv := newFakeServer()
	srv.script = idleAndFetchHeartbeatScript

	// Counter-driven dialer: fails the next N calls, then defers to inner.
	inner := srv.dialer()
	var failuresLeft atomic.Int32
	dial := func(host string, port int, useTLS bool) (*imapConn, error) {
		if failuresLeft.Load() > 0 {
			failuresLeft.Add(-1)
			return nil, fmt.Errorf("simulated dial failure")
		}
		return inner(host, port, useTLS)
	}

	m := NewManager(time.Hour).withDial(dial)
	defer m.Shutdown()

	// First Subscribe succeeds (failures=0).
	sub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Wait for initial IDLE to land.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && srv.idleStarts.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	initialStarts := srv.idleStarts.Load()
	if initialStarts < 1 {
		t.Fatal("initial IDLE never landed")
	}

	// Arm three dial failures, then drop the live conn to trigger reconnect.
	failuresLeft.Store(3)
	start := time.Now()
	srv.dropConn()

	// Wait for a fresh IDLE round on the new conn.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.idleStarts.Load() > initialStarts {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if srv.idleStarts.Load() <= initialStarts {
		t.Fatal("reconnect never produced a new IDLE round")
	}

	// Backoff should have introduced at least 10+20+40 = 70ms of delay
	// across the three failed attempts. Lower bound 50ms leaves margin for
	// scheduler granularity.
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("reconnect succeeded in %v — backoff appears bypassed", elapsed)
	}

	if got := failuresLeft.Load(); got != 0 {
		t.Errorf("expected all 3 failures consumed, %d remaining", got)
	}
}

func TestReconnectGivesUpOnDisconnect(t *testing.T) {
	prevInit := ReconnectInitialBackoff
	ReconnectInitialBackoff = 100 * time.Millisecond
	defer func() { ReconnectInitialBackoff = prevInit }()

	srv := newFakeServer()
	srv.script = idleAndFetchHeartbeatScript

	inner := srv.dialer()
	var failing atomic.Bool
	dial := func(host string, port int, useTLS bool) (*imapConn, error) {
		if failing.Load() {
			return nil, fmt.Errorf("always-fail dialer")
		}
		return inner(host, port, useTLS)
	}

	m := NewManager(time.Hour).withDial(dial)

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}

	s := sessionFor(t, m, sampleCred("inbox@example.com"))
	done := s.idleDone
	if done == nil {
		t.Fatal("idle loop should be running")
	}

	// Arm permanent failure and drop the conn.
	failing.Store(true)
	srv.dropConn()

	// Give the loop a moment to enter backoff (will sit in select on the
	// 100ms timer with the all-fail dialer).
	time.Sleep(30 * time.Millisecond)

	// Shut down: ctx cancel should unblock the backoff wait + reconnect
	// retry, and the loop should exit promptly.
	sub.Close()
	m.Shutdown()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("IDLE loop did not exit during reconnect backoff")
	}
}

func TestReconnectPreservesSinceUIDWatermark(t *testing.T) {
	// On reconnect we must keep sinceUID — otherwise mail that arrived
	// during the disconnect window is missed (re-initializing from the
	// new UIDNEXT would skip the gap).
	prevRound := IdleRoundDuration
	IdleRoundDuration = 30 * time.Second
	defer func() { IdleRoundDuration = prevRound }()

	prevBackoff := ReconnectInitialBackoff
	ReconnectInitialBackoff = 10 * time.Millisecond
	defer func() { ReconnectInitialBackoff = prevBackoff }()

	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 100)
		for {
			if !handleIdleOrFetch(fc, nil, nil) {
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	s := sessionFor(t, m, sampleCred("inbox@example.com"))
	originalSinceUID := s.sinceUID.Load()
	if originalSinceUID != 99 {
		t.Fatalf("expected initial sinceUID=99, got %d", originalSinceUID)
	}

	// Manually bump sinceUID as if FETCH had run.
	s.sinceUID.Store(150)

	srv.dropConn()

	// Wait for reconnect.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.connMu.Lock()
		alive := s.conn != nil
		s.connMu.Unlock()
		if alive && srv.idleStarts.Load() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := s.sinceUID.Load(); got != 150 {
		t.Errorf("sinceUID = %d after reconnect, want 150 (preserved)", got)
	}
	if !s.sinceUIDInitialized.Load() {
		t.Error("sinceUIDInitialized should remain true after reconnect")
	}
}
