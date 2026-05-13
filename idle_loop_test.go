package emap

import (
	"testing"
	"time"
)

func TestIdleLoopStartsWhenServerSupportsIdle(t *testing.T) {
	srv := newFakeServer()
	srv.script = idleHeartbeatScript
	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	s := sessionFor(t, m, sampleCred("alice@example.com"))
	if s.idleDone == nil {
		t.Fatal("expected idle loop running on IDLE-capable server")
	}
}

func TestIdleLoopFallsBackToPollingWhenIdleAbsent(t *testing.T) {
	// When the server doesn't advertise IDLE, the session should run a
	// polling loop instead — idleDone is non-nil (a loop IS running), but
	// its mode is poll, not IDLE. Per-mode behavior is verified in poll_test.go.
	srv := newFakeServer()
	srv.script = noIdleScript
	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	s := sessionFor(t, m, sampleCred("alice@example.com"))
	if s.idleDone == nil {
		t.Fatal("expected watch loop running in poll mode")
	}
	if s.conn.Capable("IDLE") {
		t.Fatal("test setup error: noIdleScript should not advertise IDLE")
	}
}

func TestIdleLoopHeartbeats(t *testing.T) {
	prev := IdleRoundDuration
	IdleRoundDuration = 30 * time.Millisecond
	defer func() { IdleRoundDuration = prev }()

	srv := newFakeServer()
	srv.script = idleHeartbeatScript
	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Wait long enough for ~5 heartbeat rounds; assert at least 3 to leave
	// margin for scheduler jitter.
	time.Sleep(180 * time.Millisecond)
	if got := srv.idleRounds.Load(); got < 3 {
		t.Fatalf("expected at least 3 IDLE rounds via heartbeat, got %d", got)
	}
}

func TestIdleLoopReactsToExists(t *testing.T) {
	// Long round duration so heartbeats can't accidentally bump the count;
	// only the EXISTS-triggered early exit should drive round 2.
	prev := IdleRoundDuration
	IdleRoundDuration = 30 * time.Second
	defer func() { IdleRoundDuration = prev }()

	srv := newFakeServer()
	srv.script = idleEmitOneExistsScript
	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	s := sessionFor(t, m, sampleCred("alice@example.com"))

	// idleStarts counts IDLE commands seen by the server. Round 2 may sit
	// in idle() without ever sending DONE (no heartbeat, no events), so
	// idleRounds (completed cycles) is the wrong metric here — what we
	// care about is that the client issued a second IDLE after consuming
	// the EXISTS signal.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if srv.idleStarts.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := srv.idleStarts.Load(); got < 2 {
		t.Fatalf("expected EXISTS to trigger second IDLE round, idleStarts=%d", got)
	}

	// pendingFetch should have been consumed by the loop after round 1.
	if s.pendingFetch.Load() {
		t.Fatal("pendingFetch should have been drained between rounds")
	}
}

func TestIdleLoopStopsCleanlyOnDisconnect(t *testing.T) {
	srv := newFakeServer()
	srv.script = idleHeartbeatScript
	m := NewManager(time.Hour).withDial(srv.dialer())

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}

	s := sessionFor(t, m, sampleCred("alice@example.com"))
	done := s.idleDone
	if done == nil {
		t.Fatal("idle loop should be running")
	}
	_ = sub // keep alive

	m.Shutdown()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle loop did not exit on disconnect")
	}
}

func TestIdleLoopExitsWhenServerRejectsIdle(t *testing.T) {
	// defaultScript advertises IDLE in CAPABILITY but rejects the IDLE
	// command itself with BAD. The loop should error out and exit cleanly
	// — we mainly care that the goroutine doesn't spin or leak.
	srv := newFakeServer() // default script
	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	s := sessionFor(t, m, sampleCred("alice@example.com"))
	done := s.idleDone
	if done == nil {
		t.Fatal("idle loop should have started even if it then errors")
	}

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle loop should have exited after server rejected IDLE")
	}
}
