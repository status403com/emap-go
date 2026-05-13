package emap

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPollLoopFetchesPeriodically(t *testing.T) {
	prev := PollInterval
	PollInterval = 30 * time.Millisecond
	defer func() { PollInterval = prev }()

	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleNoIdleBringUp(fc, 100)
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

	// Allow time for several poll cycles.
	time.Sleep(180 * time.Millisecond)

	if got := srv.fetchCount.Load(); got < 3 {
		t.Fatalf("expected ≥3 FETCH calls via polling, got %d", got)
	}
}

func TestPollLoopDeliversNewMail(t *testing.T) {
	prev := PollInterval
	PollInterval = 25 * time.Millisecond
	defer func() { PollInterval = prev }()

	srv := newFakeServer()
	var fetchN atomic.Int32
	srv.script = func(fc *fakeConn) {
		handleNoIdleBringUp(fc, 100)
		for {
			tag, rest, err := fc.readCommand()
			if err != nil {
				return
			}
			up := strings.ToUpper(rest)
			switch {
			case strings.HasPrefix(up, "UID FETCH"):
				n := fetchN.Add(1)
				fc.srv.fetchCount.Add(1)
				// Second poll cycle delivers the mail; subsequent are empty.
				if n == 2 {
					writeFetchResponse(fc, 1, fakeMail{
						UID:     100,
						To:      "alice@example.com",
						From:    "no-reply@vendor.com",
						Subject: "Verify",
						Body:    "code: 654321",
					})
				}
				_ = fc.write(tag + " OK FETCH completed\r\n")
			case up == "LOGOUT":
				_ = fc.write(tag + " OK bye\r\n")
				return
			default:
				_ = fc.write(tag + " BAD unknown\r\n")
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{
		To: "alice@example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	select {
	case msg, ok := <-sub.Ch:
		if !ok {
			t.Fatal("subscription channel closed")
		}
		if msg.UID != 100 {
			t.Fatalf("UID = %d, want 100", msg.UID)
		}
		if !strings.Contains(msg.Body, "654321") {
			t.Fatalf("body = %q", msg.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never received mail via polling")
	}
}

func TestPollLoopReconnectsAfterConnDrop(t *testing.T) {
	prevPoll := PollInterval
	PollInterval = 30 * time.Millisecond
	defer func() { PollInterval = prevPoll }()

	prevBackoff := ReconnectInitialBackoff
	ReconnectInitialBackoff = 10 * time.Millisecond
	defer func() { ReconnectInitialBackoff = prevBackoff }()

	srv := newFakeServer()
	var dialN atomic.Int32
	srv.script = func(fc *fakeConn) {
		n := dialN.Add(1)
		handleNoIdleBringUp(fc, 100)
		if n >= 2 {
			// Post-reconnect: deliver mail on the first FETCH.
			for {
				tag, rest, err := fc.readCommand()
				if err != nil {
					return
				}
				up := strings.ToUpper(rest)
				if strings.HasPrefix(up, "UID FETCH") {
					fc.srv.fetchCount.Add(1)
					writeFetchResponse(fc, 1, fakeMail{
						UID:     100,
						To:      "alice@example.com",
						From:    "x@y",
						Subject: "post",
						Body:    "post-reconnect poll body",
					})
					_ = fc.write(tag + " OK FETCH completed\r\n")
					return // hand off to next iteration of for in original goroutine
				}
				if up == "LOGOUT" {
					_ = fc.write(tag + " OK bye\r\n")
					return
				}
			}
		}
		// Initial dial: just ack empty fetches until conn drops.
		for {
			if !handleIdleOrFetch(fc, nil, nil) {
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

	// Let the initial poll fire once so the conn is in steady-state.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) && srv.fetchCount.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	srv.dropConn()

	select {
	case msg := <-sub.Ch:
		if msg.UID != 100 || !strings.Contains(msg.Body, "post-reconnect poll body") {
			t.Fatalf("unexpected mail: %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never received post-reconnect mail in poll mode")
	}

	if got := dialN.Load(); got < 2 {
		t.Errorf("expected ≥2 dials, got %d", got)
	}
}

func TestPollLoopStopsOnDisconnect(t *testing.T) {
	prev := PollInterval
	PollInterval = 200 * time.Millisecond
	defer func() { PollInterval = prev }()

	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleNoIdleBringUp(fc, 100)
		for {
			if !handleIdleOrFetch(fc, nil, nil) {
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{})
	if err != nil {
		t.Fatal(err)
	}

	s := sessionFor(t, m, sampleCred("inbox@example.com"))
	done := s.idleDone
	if done == nil {
		t.Fatal("watch loop (poll mode) should be running")
	}

	// Mid-PollInterval shutdown; ctx cancel must interrupt the select on
	// ticker.C, not wait the full 200ms.
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	sub.Close()
	m.Shutdown()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("poll loop did not exit on disconnect")
	}

	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("disconnect took %v — ctx cancel should interrupt poll wait promptly", elapsed)
	}
}

