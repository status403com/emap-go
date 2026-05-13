package emap

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// helper: pull one message from sub or fail after timeout.
func recvOne(t *testing.T, sub *Subscription, label string) Message {
	t.Helper()
	select {
	case msg, ok := <-sub.Ch:
		if !ok {
			t.Fatalf("%s: channel closed", label)
		}
		return msg
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("%s: timed out waiting for message", label)
		return Message{}
	}
}

// helper: verify no message arrives within a small window.
func expectNoMessage(t *testing.T, sub *Subscription, label string) {
	t.Helper()
	select {
	case msg, ok := <-sub.Ch:
		if !ok {
			t.Fatalf("%s: channel closed unexpectedly", label)
		}
		t.Fatalf("%s: unexpected message %+v", label, msg)
	case <-time.After(20 * time.Millisecond):
	}
}

func mustSubscribe(t *testing.T, m *Manager, c Credential, f Filter) *Subscription {
	t.Helper()
	sub, err := m.Subscribe(c, f)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return sub
}

// sessionFor pulls the live *session for a credential, for test injection.
func sessionFor(t *testing.T, m *Manager, c Credential) *session {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[keyFor(c)]
	if s == nil {
		t.Fatalf("no session for %s", c.Email)
	}
	return s
}

func TestDispatchDeliversToMatchingToFilter(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	subA := mustSubscribe(t, m, c, Filter{To: "alice+123@example.com"})
	subB := mustSubscribe(t, m, c, Filter{To: "bob+456@example.com"})
	defer subA.Close()
	defer subB.Close()

	s := sessionFor(t, m, c)
	s.dispatch(Message{UID: 1, To: "Alice+123@Example.com", Subject: "hi alice"})

	got := recvOne(t, subA, "subA")
	if got.Subject != "hi alice" {
		t.Fatalf("subA got wrong message: %+v", got)
	}
	expectNoMessage(t, subB, "subB")
}

func TestDispatchToWildcardSubReceivesEverything(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	wild := mustSubscribe(t, m, c, Filter{}) // no To filter
	defer wild.Close()

	s := sessionFor(t, m, c)
	s.dispatch(Message{UID: 1, To: "a@example.com"})
	s.dispatch(Message{UID: 2, To: "b@example.com"})

	for i, want := range []uint32{1, 2} {
		got := recvOne(t, wild, fmt.Sprintf("msg %d", i))
		if got.UID != want {
			t.Fatalf("expected UID %d, got %d", want, got.UID)
		}
	}
}

func TestDispatchMixedWildcardAndFilteredSubs(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	wild := mustSubscribe(t, m, c, Filter{})
	target := mustSubscribe(t, m, c, Filter{To: "alice@example.com"})
	defer wild.Close()
	defer target.Close()

	s := sessionFor(t, m, c)
	s.dispatch(Message{UID: 1, To: "alice@example.com"})

	// Both should receive.
	if msg := recvOne(t, wild, "wild"); msg.UID != 1 {
		t.Fatalf("wild got UID %d, want 1", msg.UID)
	}
	if msg := recvOne(t, target, "target"); msg.UID != 1 {
		t.Fatalf("target got UID %d, want 1", msg.UID)
	}
}

func TestDispatchFromContainsFilter(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	sub := mustSubscribe(t, m, c, Filter{FromContains: "no-reply@vendor"})
	defer sub.Close()

	s := sessionFor(t, m, c)
	s.dispatch(Message{UID: 1, From: "marketing@vendor.com", Subject: "spam"})
	s.dispatch(Message{UID: 2, From: "No-Reply@Vendor.com", Subject: "verify"})

	got := recvOne(t, sub, "verify message")
	if got.UID != 2 {
		t.Fatalf("expected UID 2 (matching), got %d", got.UID)
	}
	expectNoMessage(t, sub, "no further matches")
}

func TestDispatchSinceFilterDropsOldMail(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	cutoff := time.Now()
	sub := mustSubscribe(t, m, c, Filter{Since: cutoff})
	defer sub.Close()

	s := sessionFor(t, m, c)
	s.dispatch(Message{UID: 1, Date: cutoff.Add(-time.Hour)}) // before
	s.dispatch(Message{UID: 2, Date: cutoff.Add(time.Hour)})  // after

	got := recvOne(t, sub, "after-cutoff message")
	if got.UID != 2 {
		t.Fatalf("expected UID 2, got %d", got.UID)
	}
	expectNoMessage(t, sub, "no further")
}

func TestDispatchMaxAgeFilter(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	// Override Since so it doesn't interfere — only MaxAge gates here.
	sub := mustSubscribe(t, m, c, Filter{
		Since:  time.Now().Add(-24 * time.Hour),
		MaxAge: 5 * time.Minute,
	})
	defer sub.Close()

	s := sessionFor(t, m, c)
	s.dispatch(Message{UID: 1, Date: time.Now().Add(-time.Hour)})       // too old
	s.dispatch(Message{UID: 2, Date: time.Now().Add(-2 * time.Minute)}) // ok

	got := recvOne(t, sub, "recent message")
	if got.UID != 2 {
		t.Fatalf("expected UID 2, got %d", got.UID)
	}
	expectNoMessage(t, sub, "no further")
}

func TestDispatchToIsCaseInsensitive(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	sub := mustSubscribe(t, m, c, Filter{To: "Alice@Example.com"})
	defer sub.Close()

	s := sessionFor(t, m, c)
	s.dispatch(Message{UID: 1, To: "alice@example.com"})

	if got := recvOne(t, sub, "case-folded match"); got.UID != 1 {
		t.Fatalf("expected UID 1, got %d", got.UID)
	}
}

func TestDispatchMultipleSubsSameToAllReceive(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	sub1 := mustSubscribe(t, m, c, Filter{To: "alice@example.com"})
	sub2 := mustSubscribe(t, m, c, Filter{To: "alice@example.com"})
	defer sub1.Close()
	defer sub2.Close()

	s := sessionFor(t, m, c)
	s.dispatch(Message{UID: 1, To: "alice@example.com"})

	if got := recvOne(t, sub1, "sub1"); got.UID != 1 {
		t.Fatal("sub1 missed message")
	}
	if got := recvOne(t, sub2, "sub2"); got.UID != 1 {
		t.Fatal("sub2 missed message")
	}
}

func TestDispatchDropsOldestOnFullChannel(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	sub := mustSubscribe(t, m, c, Filter{})
	defer sub.Close()

	s := sessionFor(t, m, c)
	// subscriberBuffer = 8. Send 10 messages; the dispatcher should drop the
	// oldest to make room for the newest.
	for i := uint32(0); i < 10; i++ {
		s.dispatch(Message{UID: i})
	}

	// Drain — we should receive 8 messages, and the UID range should be
	// shifted toward the newest (drop-oldest semantics).
	received := make([]uint32, 0, subscriberBuffer)
	for {
		select {
		case msg, ok := <-sub.Ch:
			if !ok {
				t.Fatal("channel closed during drain")
			}
			received = append(received, msg.UID)
		case <-time.After(20 * time.Millisecond):
			goto done
		}
	}
done:
	if len(received) != subscriberBuffer {
		t.Fatalf("expected %d messages buffered, got %d", subscriberBuffer, len(received))
	}
	// Newest must be present.
	if received[len(received)-1] != 9 {
		t.Fatalf("expected newest UID=9 to be retained, got tail %d", received[len(received)-1])
	}
	// Oldest (UID=0) must have been dropped.
	for _, uid := range received {
		if uid == 0 {
			t.Fatal("expected oldest UID=0 to be dropped")
		}
	}
}

func TestUnsubscribeRemovesFromIndex(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")
	sub := mustSubscribe(t, m, c, Filter{To: "alice@example.com"})

	s := sessionFor(t, m, c)
	sub.Close()

	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	if len(s.subs) != 0 {
		t.Fatalf("subs map should be empty after Close, got %d", len(s.subs))
	}
	if len(s.subsByTo) != 0 {
		t.Fatalf("subsByTo should be empty after Close, got %d entries", len(s.subsByTo))
	}
}

func TestDispatchConcurrentSubscribeUnsubscribeIsRaceFree(t *testing.T) {
	m, _ := newTestManager(time.Hour)
	c := sampleCred("inbox@example.com")

	// Long-lived wildcard sub to keep the session alive throughout the test.
	keeper := mustSubscribe(t, m, c, Filter{})
	defer keeper.Close()

	s := sessionFor(t, m, c)

	stop := make(chan struct{})
	dispatcherDone := make(chan struct{})
	go func() {
		defer close(dispatcherDone)
		uid := uint32(0)
		for {
			select {
			case <-stop:
				return
			default:
				uid++
				s.dispatch(Message{UID: uid, To: "alice@example.com"})
			}
		}
	}()

	// Churners: many goroutines subscribing and unsubscribing. The dispatcher
	// is NOT in this WaitGroup — it has its own lifecycle so we can stop it
	// only after the churners finish.
	var churnersWG sync.WaitGroup
	const churners = 20
	for i := 0; i < churners; i++ {
		churnersWG.Add(1)
		go func() {
			defer churnersWG.Done()
			for j := 0; j < 50; j++ {
				sub, err := m.Subscribe(c, Filter{To: "alice@example.com"})
				if err != nil {
					return
				}
				// Drain a few then close.
				for k := 0; k < 2; k++ {
					select {
					case <-sub.Ch:
					case <-time.After(2 * time.Millisecond):
					}
				}
				sub.Close()
			}
		}()
	}

	churnersWG.Wait()
	close(stop)
	<-dispatcherDone

	// If we got here without -race tripping, dispatch + subscribe/unsubscribe
	// are properly synchronized.
}
