package emap

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ── Pure parser tests ────────────────────────────────────────────────

func TestParseFetchResponseExtractsAllFields(t *testing.T) {
	headers := []byte("To: alice@example.com\r\nFrom: bob@example.com\r\nSubject: Hello\r\nDate: Thu, 27 Feb 2026 12:34:56 +0000\r\n\r\n")
	body := []byte("Click https://example.com/verify?token=ABC123 to confirm")
	literals := [][]byte{headers, body}

	line := "* 7 FETCH (UID 42 INTERNALDATE \"27-Feb-2026 12:34:56 +0000\" BODY[HEADER.FIELDS (TO FROM SUBJECT DATE)] \x00L0\x00 BODY[TEXT] \x00L1\x00)"

	r, ok := parseFetchResponse(line, literals)
	if !ok {
		t.Fatal("parseFetchResponse returned not-ok")
	}
	if r.seq != 7 {
		t.Errorf("seq = %d, want 7", r.seq)
	}
	if r.uid != 42 {
		t.Errorf("uid = %d, want 42", r.uid)
	}
	if r.internalDate.IsZero() {
		t.Error("internalDate not parsed")
	}
	if string(r.headers) != string(headers) {
		t.Errorf("headers mismatch:\ngot:  %q\nwant: %q", r.headers, headers)
	}
	if string(r.body) != string(body) {
		t.Errorf("body mismatch:\ngot:  %q\nwant: %q", r.body, body)
	}
}

func TestParseFetchResponseSkipsNonFetchLines(t *testing.T) {
	if _, ok := parseFetchResponse("* OK [UIDNEXT 100]", nil); ok {
		t.Fatal("expected non-FETCH line to be rejected")
	}
	if _, ok := parseFetchResponse("A1 OK FETCH completed", nil); ok {
		t.Fatal("expected tagged response to be rejected")
	}
}

func TestParseHeaderFieldsBareAndAngleAddresses(t *testing.T) {
	raw := []byte("To: \"Alice\" <alice@example.com>\r\nFrom: noreply@vendor.com\r\nSubject: Verify\r\nDate: Thu, 27 Feb 2026 12:34:56 +0000\r\n\r\n")
	to, from, subject, date := parseHeaderFields(raw)
	if to != "alice@example.com" {
		t.Errorf("to = %q, want bare alice@example.com", to)
	}
	if from != "noreply@vendor.com" {
		t.Errorf("from = %q", from)
	}
	if subject != "Verify" {
		t.Errorf("subject = %q", subject)
	}
	if date.IsZero() {
		t.Error("date not parsed")
	}
}

// ── Integration: FETCH wired through the session loop ───────────────

func TestFetchDeliversNewMailToSubscriber(t *testing.T) {
	prev := IdleRoundDuration
	IdleRoundDuration = 30 * time.Second
	defer func() { IdleRoundDuration = prev }()

	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 100)
		// Round 1: emit EXISTS → triggers FETCH
		handleIdleRound(fc, []string{"* 5 EXISTS\r\n"})
		// FETCH returns one mail addressed to our subscriber
		handleFetchCommand(fc, []fakeMail{{
			UID:     100,
			To:      "alice+task1@example.com",
			From:    "no-reply@vendor.com",
			Subject: "Verify your account",
			Body:    "Code: 123456",
		}})
		// Subsequent rounds: heartbeat-only
		for {
			if !handleIdleRound(fc, nil) {
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{
		To: "alice+task1@example.com",
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
			t.Errorf("UID = %d, want 100", msg.UID)
		}
		if msg.To != "alice+task1@example.com" {
			t.Errorf("To = %q", msg.To)
		}
		if msg.From != "no-reply@vendor.com" {
			t.Errorf("From = %q", msg.From)
		}
		if !strings.Contains(msg.Body, "Code: 123456") {
			t.Errorf("Body = %q", msg.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the message")
	}
}

func TestFetchFiltersByToHeader(t *testing.T) {
	prev := IdleRoundDuration
	IdleRoundDuration = 30 * time.Second
	defer func() { IdleRoundDuration = prev }()

	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 100)
		handleIdleRound(fc, []string{"* 5 EXISTS\r\n"})
		// Two mails — one for alice, one for bob.
		handleFetchCommand(fc, []fakeMail{
			{UID: 100, To: "alice@example.com", From: "x@y", Subject: "x", Body: "x"},
			{UID: 101, To: "bob@example.com", From: "x@y", Subject: "x", Body: "x"},
		})
		for {
			if !handleIdleRound(fc, nil) {
				return
			}
		}
	}

	m := NewManager(time.Hour).withDial(srv.dialer())
	defer m.Shutdown()

	aliceSub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{To: "alice@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	defer aliceSub.Close()

	// alice should receive UID 100, NOT 101
	select {
	case msg := <-aliceSub.Ch:
		if msg.UID != 100 {
			t.Fatalf("alice expected UID 100, got %d", msg.UID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("alice never received mail")
	}

	// No second message for alice
	select {
	case msg := <-aliceSub.Ch:
		t.Fatalf("alice received unexpected second message UID %d", msg.UID)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSinceUIDSkipsBacklogOnConnect(t *testing.T) {
	// Server reports UIDNEXT = 1_000_000 — meaning ~1M existing messages.
	// Our session must NOT FETCH any of them at connect, only on subsequent
	// EXISTS events.
	srv := newFakeServer()
	var fetches atomic.Int32
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 1_000_000)
		// Loop reading commands. Count any FETCH (there should be 0).
		for {
			tag, rest, err := fc.readCommand()
			if err != nil {
				return
			}
			up := strings.ToUpper(rest)
			switch {
			case up == "IDLE":
				if fc.srv != nil {
					fc.srv.idleStarts.Add(1)
				}
				_ = fc.write("+ idling\r\n")
				_ = fc.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				line, err := fc.r.ReadString('\n')
				_ = fc.conn.SetReadDeadline(time.Time{})
				if err != nil {
					return
				}
				if !strings.EqualFold(strings.TrimSpace(line), "DONE") {
					return
				}
				_ = fc.write(tag + " OK IDLE completed\r\n")
			case strings.HasPrefix(up, "UID FETCH") || strings.HasPrefix(up, "FETCH"):
				fetches.Add(1)
				_ = fc.write(tag + " OK FETCH completed\r\n")
			case up == "LOGOUT":
				_ = fc.write(tag + " OK bye\r\n")
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

	// Verify sinceUID was initialized to UIDNEXT-1 = 999_999.
	s := sessionFor(t, m, sampleCred("inbox@example.com"))
	if got := s.sinceUID.Load(); got != 999_999 {
		t.Errorf("sinceUID = %d, want 999_999", got)
	}
	if !s.sinceUIDInitialized.Load() {
		t.Error("sinceUIDInitialized should be true after UIDNEXT")
	}

	// Wait a bit; verify no FETCH was issued (no EXISTS happened).
	time.Sleep(150 * time.Millisecond)
	if got := fetches.Load(); got != 0 {
		t.Fatalf("expected 0 FETCH commands on connect, got %d", got)
	}
}

func TestSinceUIDRefusesFetchWhenServerOmitsUIDNext(t *testing.T) {
	// Some non-conformant servers don't return UIDNEXT. We must NOT
	// attempt UID FETCH 1:* in that case — that would drag the entire
	// inbox down. Test verifies fetchNewMessages bails out cleanly.
	prev := IdleRoundDuration
	IdleRoundDuration = 30 * time.Second
	defer func() { IdleRoundDuration = prev }()

	srv := newFakeServer()
	var fetches atomic.Int32
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 0) // 0 = don't emit UIDNEXT
		// First round emits EXISTS → session WOULD normally FETCH.
		handleIdleRound(fc, []string{"* 1 EXISTS\r\n"})
		// Count any FETCH attempt (should not happen).
		for {
			tag, rest, err := fc.readCommand()
			if err != nil {
				return
			}
			up := strings.ToUpper(rest)
			switch {
			case strings.HasPrefix(up, "UID FETCH") || strings.HasPrefix(up, "FETCH"):
				fetches.Add(1)
				_ = fc.write(tag + " OK FETCH completed\r\n")
			case up == "IDLE":
				if fc.srv != nil {
					fc.srv.idleStarts.Add(1)
				}
				_ = fc.write("+ idling\r\n")
				_ = fc.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
				line, err := fc.r.ReadString('\n')
				_ = fc.conn.SetReadDeadline(time.Time{})
				if err != nil {
					return
				}
				if !strings.EqualFold(strings.TrimSpace(line), "DONE") {
					return
				}
				_ = fc.write(tag + " OK IDLE completed\r\n")
			case up == "LOGOUT":
				_ = fc.write(tag + " OK bye\r\n")
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
	if s.sinceUIDInitialized.Load() {
		t.Fatal("sinceUIDInitialized should be false when server omits UIDNEXT")
	}

	// Wait for EXISTS to land and the loop to evaluate handlePendingFetch.
	time.Sleep(200 * time.Millisecond)
	if got := fetches.Load(); got != 0 {
		t.Fatalf("expected 0 FETCH attempts when UIDNEXT missing, got %d", got)
	}
}

func TestFetchUpdatesSinceUID(t *testing.T) {
	prev := IdleRoundDuration
	IdleRoundDuration = 30 * time.Second
	defer func() { IdleRoundDuration = prev }()

	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 100)
		handleIdleRound(fc, []string{"* 5 EXISTS\r\n"})
		handleFetchCommand(fc, []fakeMail{
			{UID: 100, To: "a@x", From: "b@y", Subject: "s", Body: "b"},
			{UID: 102, To: "a@x", From: "b@y", Subject: "s", Body: "b"},
			{UID: 101, To: "a@x", From: "b@y", Subject: "s", Body: "b"}, // out-of-order
		})
		for {
			if !handleIdleRound(fc, nil) {
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

	// Drain three messages, then check watermark.
	for i := 0; i < 3; i++ {
		select {
		case <-sub.Ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("missed message %d", i)
		}
	}

	s := sessionFor(t, m, sampleCred("inbox@example.com"))
	if got := s.sinceUID.Load(); got != 102 {
		t.Fatalf("sinceUID = %d, want 102 (max of fetched UIDs)", got)
	}
}

// readFetchLine round-trip — feed it a hand-built FETCH wire response and
// verify the placeholder substitution + literal capture.
func TestReadFetchLineCapturesMultipleLiterals(t *testing.T) {
	srv := newFakeServer()
	srv.script = func(fc *fakeConn) {
		handleIdleBringUpWithUIDNext(fc, 1)
		handleFetchCommand(fc, []fakeMail{{
			UID:     1,
			To:      "a@x",
			From:    "b@y",
			Subject: "s",
			Body:    "Body with ) parens and \"quotes\" and {fake} markers",
		}})
		for {
			if _, _, err := fc.readCommand(); err != nil {
				return
			}
		}
	}

	c, err := srv.dialer()("h", 993, true)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.login("a", "p"); err != nil {
		t.Fatal(err)
	}
	if err := c.capability(); err != nil {
		t.Fatal(err)
	}
	if err := c.selectFolder("INBOX"); err != nil {
		t.Fatal(err)
	}
	resps, err := c.execFetch("UID FETCH 1:* (UID INTERNALDATE BODY.PEEK[HEADER.FIELDS (TO FROM SUBJECT DATE)] BODY.PEEK[TEXT])")
	if err != nil {
		t.Fatal(err)
	}
	if len(resps) != 1 {
		t.Fatalf("expected 1 response, got %d", len(resps))
	}
	r := resps[0]
	if r.uid != 1 {
		t.Errorf("uid = %d", r.uid)
	}
	if !strings.Contains(string(r.body), "{fake}") {
		t.Errorf("body literal not preserved verbatim: %q", r.body)
	}
	if !strings.Contains(string(r.body), ") parens") {
		t.Errorf("paren in body not preserved: %q", r.body)
	}
}

