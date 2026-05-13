package emap

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sampleCred(email string) Credential {
	return Credential{
		Email:    email,
		Password: "pw",
		Host:     "imap.example.com",
		Port:     993,
		UseTLS:   true,
	}
}

// newTestManager wires a Manager to a default fake IMAP server so tests don't
// hit real DNS or networks. Each session that gets created spawns one fake
// server goroutine; closing the manager (Shutdown) tears them all down.
func newTestManager(linger time.Duration) (*Manager, *fakeServer) {
	srv := newFakeServer()
	return NewManager(linger).withDial(srv.dialer()), srv
}

func TestSubscribeCreatesOneSessionPerCredential(t *testing.T) {
	m, _ := newTestManager(50 * time.Millisecond)
	c := sampleCred("alice@example.com")

	sub1, err := m.Subscribe(c, Filter{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub1.Close()

	sub2, err := m.Subscribe(c, Filter{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub2.Close()

	if got := m.activeSessions(); got != 1 {
		t.Fatalf("expected 1 session, got %d", got)
	}
}

func TestSubscribeDifferentCredentialsAllocateSeparateSessions(t *testing.T) {
	m, _ := newTestManager(50 * time.Millisecond)

	sub1, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatalf("Subscribe alice: %v", err)
	}
	defer sub1.Close()

	sub2, err := m.Subscribe(sampleCred("bob@example.com"), Filter{})
	if err != nil {
		t.Fatalf("Subscribe bob: %v", err)
	}
	defer sub2.Close()

	if got := m.activeSessions(); got != 2 {
		t.Fatalf("expected 2 sessions, got %d", got)
	}
}

func TestEmailCaseDoesNotForkSessions(t *testing.T) {
	m, _ := newTestManager(50 * time.Millisecond)

	sub1, err := m.Subscribe(sampleCred("Alice@Example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Close()

	sub2, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()

	if got := m.activeSessions(); got != 1 {
		t.Fatalf("expected 1 session for case-different emails, got %d", got)
	}
}

func TestCloseSchedulesLingerThenTearsDown(t *testing.T) {
	const linger = 30 * time.Millisecond
	m, _ := newTestManager(linger)

	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	sub.Close()

	// Immediately after Close, session is still alive (within linger).
	if got := m.activeSessions(); got != 1 {
		t.Fatalf("expected session to linger, got %d active", got)
	}

	// Wait past linger; session should be gone.
	time.Sleep(linger * 4)
	if got := m.activeSessions(); got != 0 {
		t.Fatalf("expected session to be torn down, got %d active", got)
	}
}

func TestSubscribeDuringLingerCancelsTeardown(t *testing.T) {
	const linger = 50 * time.Millisecond
	m, _ := newTestManager(linger)
	c := sampleCred("alice@example.com")

	sub1, err := m.Subscribe(c, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	sub1.Close()

	// Subscribe again before linger expires.
	time.Sleep(linger / 2)
	sub2, err := m.Subscribe(c, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()

	// Wait past what would have been the original linger expiry.
	time.Sleep(linger * 2)
	if got := m.activeSessions(); got != 1 {
		t.Fatalf("expected session to survive linger cancel, got %d active", got)
	}
}

func TestDoubleCloseIsSafe(t *testing.T) {
	m, _ := newTestManager(20 * time.Millisecond)
	sub, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	sub.Close()
	sub.Close() // must not panic, must not double-decrement
	time.Sleep(60 * time.Millisecond)
	if got := m.activeSessions(); got != 0 {
		t.Fatalf("expected session torn down, got %d active", got)
	}
}

func TestShutdownClosesAllSessionsAndChannels(t *testing.T) {
	m, _ := newTestManager(time.Hour) // long linger so we know Shutdown is doing the work

	sub1, err := m.Subscribe(sampleCred("alice@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := m.Subscribe(sampleCred("bob@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}

	m.Shutdown()

	if got := m.activeSessions(); got != 0 {
		t.Fatalf("expected 0 sessions after Shutdown, got %d", got)
	}
	// Channels should be closed — reading yields zero value with !ok.
	for _, sub := range []*Subscription{sub1, sub2} {
		select {
		case _, ok := <-sub.Ch:
			if ok {
				t.Fatal("expected closed channel after Shutdown")
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Ch did not close within timeout")
		}
	}
}

func TestConcurrentSubscribeCloseStaysClean(t *testing.T) {
	const linger = 5 * time.Millisecond
	m, _ := newTestManager(linger)
	c := sampleCred("alice@example.com")

	const goroutines = 100
	const itersPerG = 50

	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < itersPerG; j++ {
				sub, err := m.Subscribe(c, Filter{})
				if err != nil {
					failures.Add(1)
					return
				}
				sub.Close()
			}
		}()
	}
	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d subscribe failures under concurrency", n)
	}

	// Drain any pending linger.
	time.Sleep(linger * 5)
	if got := m.activeSessions(); got != 0 {
		t.Fatalf("expected 0 sessions after all unsubscribes, got %d", got)
	}
}

func TestSubscribeReusesLingeringConnection(t *testing.T) {
	// Verifies the session pointer is preserved across linger reuse — same
	// underlying TCP/LOGIN state would be reused once Phase 1B wires it up.
	const linger = 50 * time.Millisecond
	m, _ := newTestManager(linger)
	c := sampleCred("alice@example.com")

	sub1, err := m.Subscribe(c, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	first := m.sessions[keyFor(c)]
	m.mu.Unlock()
	sub1.Close()

	time.Sleep(linger / 2)

	sub2, err := m.Subscribe(c, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()
	m.mu.Lock()
	second := m.sessions[keyFor(c)]
	m.mu.Unlock()

	if first != second {
		t.Fatal("expected lingering session to be reused, got a fresh instance")
	}
}
