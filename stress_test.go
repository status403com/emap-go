package emap

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStressConnectionPoolingAtScale subscribes 2000 tasks against 5 unique
// credentials and verifies the manager opens exactly 5 TCP connections —
// not 2000. This is the core efficiency promise: per-credential pooling
// regardless of subscriber count.
func TestStressConnectionPoolingAtScale(t *testing.T) {
	prev := IdleRoundDuration
	IdleRoundDuration = 200 * time.Millisecond
	defer func() { IdleRoundDuration = prev }()

	const (
		credentials       = 5
		subsPerCredential = 400
		totalSubs         = credentials * subsPerCredential // 2000
	)

	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 1)
		for {
			if !handleIdleOrFetch(fc, nil, nil) {
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	subs := make([]*Subscription, 0, totalSubs)
	for i := 0; i < totalSubs; i++ {
		credIdx := i % credentials
		cred := sampleCred(fmt.Sprintf("inbox%d@example.com", credIdx))
		sub, err := m.Subscribe(cred, Filter{
			To: fmt.Sprintf("task%d@example.com", i),
		})
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
		subs = append(subs, sub)
	}

	if got := m.activeSessions(); got != credentials {
		t.Fatalf("expected %d sessions (one per credential), got %d", credentials, got)
	}
	if got := srv.dialsCt.Load(); got != int32(credentials) {
		t.Fatalf("expected %d dials, got %d", credentials, got)
	}

	// Give IDLE loops time to issue at least one round each.
	time.Sleep(300 * time.Millisecond)

	// New Subscribes to the same credentials must reuse existing sessions.
	extra := make([]*Subscription, credentials)
	for i := 0; i < credentials; i++ {
		cred := sampleCred(fmt.Sprintf("inbox%d@example.com", i))
		sub, err := m.Subscribe(cred, Filter{})
		if err != nil {
			t.Fatalf("extra Subscribe[%d]: %v", i, err)
		}
		extra[i] = sub
	}
	if got := srv.dialsCt.Load(); got != int32(credentials) {
		t.Errorf("extra subs caused new dials: total=%d, want %d", got, credentials)
	}

	for _, sub := range subs {
		sub.Close()
	}
	for _, sub := range extra {
		sub.Close()
	}
}

// TestStressNoGoroutineLeakAfterShutdown verifies that closing all
// subscriptions and shutting down returns the goroutine count to its
// starting baseline — no orphaned IDLE loops, watchers, server scripts,
// or readers left dangling.
func TestStressNoGoroutineLeakAfterShutdown(t *testing.T) {
	prev := IdleRoundDuration
	IdleRoundDuration = 100 * time.Millisecond
	defer func() { IdleRoundDuration = prev }()

	const (
		credentials       = 4
		subsPerCredential = 250
	)

	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 1)
		for {
			if !handleIdleOrFetch(fc, nil, nil) {
				return
			}
		}
	}

	// Stabilize: let any pending GC / scheduler activity settle.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	m := NewManager(time.Hour).withDial(srv.dialer())

	subs := make([]*Subscription, 0, credentials*subsPerCredential)
	for i := 0; i < credentials*subsPerCredential; i++ {
		cred := sampleCred(fmt.Sprintf("inbox%d@example.com", i%credentials))
		sub, err := m.Subscribe(cred, Filter{To: fmt.Sprintf("t%d@x", i)})
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
		subs = append(subs, sub)
	}

	// Run for a few IDLE rounds so reader/writer goroutines are active.
	time.Sleep(300 * time.Millisecond)

	for _, sub := range subs {
		sub.Close()
	}
	m.Shutdown()

	// Wait for all watch + server goroutines to settle.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		current := runtime.NumGoroutine()
		// Allow tiny slack for test infrastructure timers.
		if current <= baseline+2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	final := runtime.NumGoroutine()
	if delta := final - baseline; delta > 5 {
		t.Fatalf("goroutine leak: baseline=%d final=%d delta=%d", baseline, final, delta)
	}
}

// TestStressDispatchUnderConcurrentLoad fires off a burst of mail across
// 4 credentials and 800 subscribers, then verifies that each per-task
// targeted message reached exactly the right subscriber. Catches any
// race in the dispatcher under realistic fanout.
func TestStressDispatchUnderConcurrentLoad(t *testing.T) {
	prev := IdleRoundDuration
	IdleRoundDuration = 30 * time.Second // we drive arrivals manually
	defer func() { IdleRoundDuration = prev }()

	const (
		credentials       = 4
		subsPerCredential = 200
	)

	// Map each cred index to its session so we can dispatch directly.
	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 1)
		for {
			if !handleIdleOrFetch(fc, nil, nil) {
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	// Subscribe with per-task To filters.
	subs := make(map[string]*Subscription, credentials*subsPerCredential)
	for i := 0; i < credentials*subsPerCredential; i++ {
		credIdx := i % credentials
		cred := sampleCred(fmt.Sprintf("inbox%d@example.com", credIdx))
		taskAddr := fmt.Sprintf("task%d@example.com", i)
		sub, err := m.Subscribe(cred, Filter{To: taskAddr})
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
		subs[taskAddr] = sub
	}

	// Dispatch directly via session — no FETCH round-trip needed for the
	// dispatch-correctness test.
	sessionByCred := make(map[int]*session, credentials)
	for i := 0; i < credentials; i++ {
		s := sessionFor(t, m, sampleCred(fmt.Sprintf("inbox%d@example.com", i)))
		sessionByCred[i] = s
	}

	// Fire one message per task in parallel across all credentials.
	var wg sync.WaitGroup
	for i := 0; i < credentials*subsPerCredential; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			credIdx := i % credentials
			s := sessionByCred[credIdx]
			taskAddr := fmt.Sprintf("task%d@example.com", i)
			s.dispatch(Message{
				UID:     uint32(1000 + i),
				To:      taskAddr,
				From:    "no-reply@vendor.com",
				Subject: "verify",
				Date:    time.Now(),
				Body:    fmt.Sprintf("code-%d", i),
			})
		}(i)
	}
	wg.Wait()

	// Drain: each sub should receive its own message.
	var received atomic.Int32
	var wgDrain sync.WaitGroup
	for taskAddr, sub := range subs {
		wgDrain.Add(1)
		go func(taskAddr string, sub *Subscription) {
			defer wgDrain.Done()
			select {
			case msg := <-sub.Ch:
				if msg.To != taskAddr {
					t.Errorf("%s: received mail for %q", taskAddr, msg.To)
					return
				}
				wantBody := "code-" + strings.TrimPrefix(strings.TrimSuffix(taskAddr, "@example.com"), "task")
				if !strings.Contains(msg.Body, wantBody) {
					t.Errorf("%s: body = %q, want suffix %s", taskAddr, msg.Body, wantBody)
					return
				}
				received.Add(1)
			case <-time.After(2 * time.Second):
				t.Errorf("%s: no message received", taskAddr)
			}
		}(taskAddr, sub)
	}
	wgDrain.Wait()

	if got := int(received.Load()); got != credentials*subsPerCredential {
		t.Fatalf("expected %d delivered messages, got %d", credentials*subsPerCredential, got)
	}

	for _, sub := range subs {
		sub.Close()
	}
}

// TestStressLingerReusesConnectionsAcrossChurn rapidly creates and closes
// subscriptions to the same credential, exercising the linger pooling.
// The dial count should stay at 1 throughout — never bouncing — because
// the 60s linger keeps the session warm across the rapid churn.
func TestStressLingerReusesConnectionsAcrossChurn(t *testing.T) {
	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 1)
		for {
			if !handleIdleOrFetch(fc, nil, nil) {
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	cred := sampleCred("inbox@example.com")
	const iterations = 5000
	for i := 0; i < iterations; i++ {
		sub, err := m.Subscribe(cred, Filter{})
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
		sub.Close()
	}

	if got := srv.dialsCt.Load(); got != 1 {
		t.Fatalf("expected exactly 1 dial across %d churn cycles, got %d", iterations, got)
	}
}
