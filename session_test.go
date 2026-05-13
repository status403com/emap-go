package emap

import (
	"testing"
	"time"
)

func TestSessionConnectHappyPath(t *testing.T) {
	srv := newFakeServer()
	m := NewManager(50 * time.Millisecond).withDial(srv.dialer())

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	m.mu.Lock()
	s := m.sessions[keyFor(sampleCred("alice@example.com"))]
	m.mu.Unlock()
	if s == nil {
		t.Fatal("no session after Subscribe")
	}
	if got := s.state.Load(); got != sessionStateActive {
		t.Fatalf("expected state Active, got %d", got)
	}
	s.connMu.Lock()
	if s.conn == nil {
		t.Fatal("expected live conn on active session")
	}
	if s.selectedFolder != inboxFolder {
		t.Fatalf("expected selectedFolder=INBOX, got %q", s.selectedFolder)
	}
	if !s.conn.Capable("IDLE") {
		t.Fatal("expected IDLE capability advertised by default script")
	}
	if !s.conn.Capable("LITERAL+") {
		t.Fatal("expected LITERAL+ capability")
	}
	s.connMu.Unlock()
}

func TestSessionConnectFailsOnBadLogin(t *testing.T) {
	srv := newFakeServer()
	srv.script = rejectLoginScript
	m := NewManager(50 * time.Millisecond).withDial(srv.dialer())

	_, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err == nil {
		t.Fatal("expected Subscribe to fail when LOGIN is rejected")
	}
	// No session should be left in the map after a failed connect.
	if got := m.activeSessions(); got != 0 {
		t.Fatalf("expected 0 sessions after auth failure, got %d", got)
	}
}

func TestSessionConnectFailsOnBadGreeting(t *testing.T) {
	srv := newFakeServer()
	srv.script = badGreetingScript
	m := NewManager(50 * time.Millisecond).withDial(srv.dialer())

	_, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err == nil {
		t.Fatal("expected Subscribe to fail on bad greeting")
	}
	if got := m.activeSessions(); got != 0 {
		t.Fatalf("expected 0 sessions, got %d", got)
	}
}

func TestCapabilityWithoutIdleIsObservable(t *testing.T) {
	srv := newFakeServer()
	srv.script = noIdleScript
	m := NewManager(50 * time.Millisecond).withDial(srv.dialer())

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	m.mu.Lock()
	s := m.sessions[keyFor(sampleCred("alice@example.com"))]
	m.mu.Unlock()
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn.Capable("IDLE") {
		t.Fatal("expected IDLE NOT advertised by noIdleScript")
	}
}

func TestDisconnectClosesConnAndChannels(t *testing.T) {
	srv := newFakeServer()
	m := NewManager(20 * time.Millisecond).withDial(srv.dialer())

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	sub.Close() // trigger linger

	time.Sleep(60 * time.Millisecond)

	if got := m.activeSessions(); got != 0 {
		t.Fatalf("expected session torn down, got %d", got)
	}
	// Subscription channel should be closed.
	select {
	case _, ok := <-sub.Ch:
		if ok {
			t.Fatal("expected closed channel after teardown")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Ch did not close")
	}
}

func TestReconnectRestoresActiveState(t *testing.T) {
	srv := newFakeServer()
	m := NewManager(time.Hour).withDial(srv.dialer())

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	m.mu.Lock()
	s := m.sessions[keyFor(sampleCred("alice@example.com"))]
	m.mu.Unlock()

	// Simulate connection drop and request reconnect.
	srv.dropConn()
	if err := s.reconnect(); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if got := s.state.Load(); got != sessionStateActive {
		t.Fatalf("expected Active after reconnect, got %d", got)
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil {
		t.Fatal("expected live conn after reconnect")
	}
	if s.selectedFolder != inboxFolder {
		t.Fatalf("expected INBOX selected after reconnect, got %q", s.selectedFolder)
	}
}

func TestReconnectAfterCloseFails(t *testing.T) {
	srv := newFakeServer()
	m := NewManager(time.Hour).withDial(srv.dialer())

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}

	m.mu.Lock()
	s := m.sessions[keyFor(sampleCred("alice@example.com"))]
	m.mu.Unlock()

	sub.Close()
	m.Shutdown() // force close

	if err := s.reconnect(); err == nil {
		t.Fatal("expected reconnect to fail on closed session")
	}
}

func TestShutdownClosesActiveConnections(t *testing.T) {
	srv := newFakeServer()
	m := NewManager(time.Hour).withDial(srv.dialer())

	_, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Subscribe(sampleCred("bob@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}

	if got := srv.dialsCt.Load(); got != 2 {
		t.Fatalf("expected 2 dials, got %d", got)
	}

	m.Shutdown()

	if got := m.activeSessions(); got != 0 {
		t.Fatalf("expected 0 sessions after Shutdown, got %d", got)
	}
}
