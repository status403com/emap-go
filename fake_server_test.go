package emap

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// fakeServer is a minimal scripted IMAP server backed by net.Pipe — no real
// TCP, no flakiness. Each new "connection" runs script() in a goroutine; the
// dialFunc returned via dialer() hands the client end to the imap session.
//
// script receives a *fakeConn that reads tagged commands and writes scripted
// responses. The default script (defaultScript) handles greeting + LOGIN +
// CAPABILITY + SELECT + LOGOUT for vanilla happy-path tests.
type fakeServer struct {
	script func(*fakeConn)

	mu       sync.Mutex
	openConn net.Conn // most recent client-side conn; tests use to inspect/close
	dialsCt  atomic.Int32

	// idleRounds is incremented by handleIdleRound on every IDLE → DONE
	// cycle the script services. Tests read it to verify the session-level
	// IDLE loop is re-issuing as expected.
	idleRounds atomic.Int32

	// idleStarts is incremented whenever the server sees an IDLE command
	// (before any DONE). Useful when a test wants to confirm the client
	// issued a new IDLE without requiring it to also send DONE.
	idleStarts atomic.Int32

	// fetchCount is incremented by handleIdleOrFetch on every UID FETCH /
	// FETCH command the server receives. Phase 3 poll tests use it to
	// verify the polling cadence.
	fetchCount atomic.Int32
}

func newFakeServer() *fakeServer {
	return &fakeServer{script: defaultScript}
}

// dialer returns a dialFunc matching the session's expected signature.
// Each call to it spawns a fresh server goroutine and returns a ready conn.
func (f *fakeServer) dialer() dialFunc {
	return func(host string, port int, useTLS bool) (*imapConn, error) {
		f.dialsCt.Add(1)
		clientSide, serverSide := net.Pipe()
		f.mu.Lock()
		f.openConn = clientSide
		f.mu.Unlock()
		go func() {
			fc := &fakeConn{conn: serverSide, r: bufio.NewReader(serverSide), srv: f}
			f.script(fc)
			_ = serverSide.Close()
		}()
		return wrapConnAndReadGreeting(clientSide)
	}
}

// dropConn closes the most recent client connection so the session sees a
// broken pipe on its next exec. Used to exercise reconnect.
func (f *fakeServer) dropConn() {
	f.mu.Lock()
	c := f.openConn
	f.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

// fakeConn wraps a server-side net.Pipe end with line buffering. The script
// uses it to read tagged client commands and write canned responses.
type fakeConn struct {
	conn net.Conn
	r    *bufio.Reader
	srv  *fakeServer // back-reference for shared counters
}

// readCommand reads one CRLF-terminated client command, returning (tag, rest).
// Returns ("", "", io.EOF) when the client disconnects.
func (f *fakeConn) readCommand() (string, string, error) {
	_ = f.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := f.r.ReadString('\n')
	_ = f.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return "", "", err
	}
	line = strings.TrimRight(line, "\r\n")
	sp := strings.IndexByte(line, ' ')
	if sp < 0 {
		return line, "", nil
	}
	return line[:sp], line[sp+1:], nil
}

func (f *fakeConn) write(s string) error {
	_, err := io.WriteString(f.conn, s)
	return err
}

// defaultScript answers GREETING + the four bring-up commands and LOGOUT.
// Closes its end of the pipe on LOGOUT or read error.
func defaultScript(fc *fakeConn) {
	_ = fc.write("* OK fake imap ready\r\n")
	for {
		tag, rest, err := fc.readCommand()
		if err != nil {
			return
		}
		up := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(up, "LOGIN "):
			_ = fc.write(tag + " OK LOGIN completed\r\n")
		case up == "CAPABILITY":
			_ = fc.write("* CAPABILITY IMAP4rev1 IDLE LITERAL+\r\n")
			_ = fc.write(tag + " OK CAPABILITY completed\r\n")
		case strings.HasPrefix(up, "SELECT "):
			_ = fc.write("* 12 EXISTS\r\n")
			_ = fc.write("* 0 RECENT\r\n")
			_ = fc.write("* OK [UIDNEXT 13] mailbox ready\r\n")
			_ = fc.write(tag + " OK [READ-WRITE] SELECT completed\r\n")
		case up == "LOGOUT":
			_ = fc.write("* BYE goodbye\r\n")
			_ = fc.write(tag + " OK LOGOUT completed\r\n")
			return
		default:
			_ = fc.write(tag + " BAD unknown\r\n")
		}
	}
}

// rejectLoginScript fails LOGIN with NO. Used for auth-failure tests.
func rejectLoginScript(fc *fakeConn) {
	_ = fc.write("* OK fake imap ready\r\n")
	for {
		tag, rest, err := fc.readCommand()
		if err != nil {
			return
		}
		up := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(up, "LOGIN "):
			_ = fc.write(tag + " NO authentication failed\r\n")
			return
		default:
			_ = fc.write(tag + " BAD unknown\r\n")
		}
	}
}

// badGreetingScript sends a non-OK greeting and disconnects.
func badGreetingScript(fc *fakeConn) {
	_ = fc.write("* BYE service temporarily unavailable\r\n")
}

// idleNoEventsScript handles IDLE by sending `+ idling` and waiting for DONE
// without emitting any untagged responses. Used to verify ctx-driven exit.
func idleNoEventsScript(fc *fakeConn) {
	handleIdleBringUp(fc)
	handleIdle(fc, nil)
}

// idleWithEventsScript emits two EXISTS events after `+ idling`, then waits
// for DONE. Used to verify handler dispatch.
func idleWithEventsScript(fc *fakeConn) {
	handleIdleBringUp(fc)
	handleIdle(fc, []string{"* 5 EXISTS\r\n", "* 6 EXISTS\r\n"})
}

// idleHeartbeatScript handles a continuous stream of IDLE rounds without
// emitting events. Used to verify the session-level IDLE loop re-issues on
// each IdleRoundDuration heartbeat.
func idleHeartbeatScript(fc *fakeConn) {
	handleIdleBringUp(fc)
	for {
		if !handleIdleRound(fc, nil) {
			return
		}
	}
}

// idleAndFetchHeartbeatScript handles whatever the client sends — IDLE or
// UID FETCH. Useful for reconnect tests where the session loop force-FETCHes
// after reconnecting before re-entering IDLE.
func idleAndFetchHeartbeatScript(fc *fakeConn) {
	handleIdleBringUp(fc)
	for {
		if !handleIdleOrFetch(fc, nil, nil) {
			return
		}
	}
}

// handleIdleOrFetch services one client command. Accepts either IDLE (with
// optional pre-events) or UID FETCH (with optional mail to deliver).
// Returns false on LOGOUT/error.
func handleIdleOrFetch(fc *fakeConn, idleEvents []string, fetchMail []fakeMail) bool {
	tag, rest, err := fc.readCommand()
	if err != nil {
		return false
	}
	up := strings.ToUpper(rest)
	switch {
	case up == "IDLE":
		if fc.srv != nil {
			fc.srv.idleStarts.Add(1)
		}
		_ = fc.write("+ idling\r\n")
		for _, e := range idleEvents {
			_ = fc.write(e)
		}
		_ = fc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := fc.r.ReadString('\n')
		_ = fc.conn.SetReadDeadline(time.Time{})
		if err != nil {
			return false
		}
		if !strings.EqualFold(strings.TrimSpace(line), "DONE") {
			return false
		}
		_ = fc.write(tag + " OK IDLE completed\r\n")
		if fc.srv != nil {
			fc.srv.idleRounds.Add(1)
		}
		return true
	case strings.HasPrefix(up, "UID FETCH") || strings.HasPrefix(up, "FETCH"):
		if fc.srv != nil {
			fc.srv.fetchCount.Add(1)
		}
		for i, m := range fetchMail {
			writeFetchResponse(fc, i+1, m)
		}
		_ = fc.write(tag + " OK FETCH completed\r\n")
		return true
	case up == "LOGOUT":
		_ = fc.write(tag + " OK bye\r\n")
		return false
	default:
		_ = fc.write(tag + " BAD unknown\r\n")
		return false
	}
}

// idleEmitOneExistsScript fires a single EXISTS during the first IDLE round,
// then services the FETCH that follows and goes back to heartbeat IDLE.
// Used to verify the loop reacts to EXISTS by exiting IDLE early and
// re-issuing after the FETCH completes.
func idleEmitOneExistsScript(fc *fakeConn) {
	handleIdleBringUp(fc)
	handleIdleRound(fc, []string{"* 5 EXISTS\r\n"})
	// After EXISTS, the session loop issues UID FETCH. Ack with no results.
	handleFetchCommand(fc, nil)
	for {
		if !handleIdleRound(fc, nil) {
			return
		}
	}
}

// fakeMail describes one message the fake server will return from a FETCH.
type fakeMail struct {
	UID          uint32
	To           string
	From         string
	Subject      string
	Date         string // RFC 5322 header value, e.g. "Thu, 27 Feb 2026 12:34:56 +0000"
	InternalDate string // IMAP wire format, e.g. "27-Feb-2026 12:34:56 +0000"
	Body         string
}

// handleFetchCommand reads one UID FETCH command and writes the given mail
// back as `* N FETCH (...)` responses, terminated by a tagged OK. Returns
// false on read error or unexpected command.
func handleFetchCommand(fc *fakeConn, mail []fakeMail) bool {
	tag, rest, err := fc.readCommand()
	if err != nil {
		return false
	}
	up := strings.ToUpper(rest)
	if !strings.HasPrefix(up, "UID FETCH") && !strings.HasPrefix(up, "FETCH") {
		_ = fc.write(tag + " BAD expected FETCH\r\n")
		return false
	}
	for i, m := range mail {
		writeFetchResponse(fc, i+1, m)
	}
	_ = fc.write(tag + " OK FETCH completed\r\n")
	return true
}

// writeFetchResponse formats one `* N FETCH (...)` line including literal
// blocks for headers and body, matching what a real IMAP server emits for
// our UID FETCH (UID INTERNALDATE BODY.PEEK[HEADER.FIELDS (...)] BODY.PEEK[TEXT])
// command.
func writeFetchResponse(fc *fakeConn, seq int, m fakeMail) {
	if m.InternalDate == "" {
		// Default to "now" so tests don't accidentally get filtered out by
		// Filter.Since (which defaults to subscribe time).
		m.InternalDate = time.Now().UTC().Format(internalDateLayout)
	}
	if m.Date == "" {
		m.Date = time.Now().UTC().Format(time.RFC1123Z)
	}
	headers := fmt.Sprintf("To: %s\r\nFrom: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n",
		m.To, m.From, m.Subject, m.Date)
	body := m.Body

	_ = fc.write(fmt.Sprintf(
		"* %d FETCH (UID %d INTERNALDATE \"%s\" BODY[HEADER.FIELDS (TO FROM SUBJECT DATE)] {%d}\r\n%s BODY[TEXT] {%d}\r\n%s)\r\n",
		seq, m.UID, m.InternalDate, len(headers), headers, len(body), body,
	))
}

// handleIdleRound services one IDLE → DONE cycle for scripts that loop. Same
// semantics as handleIdle but returns false on LOGOUT/error so the caller
// knows when to stop. Increments srv.idleRounds for each completed round.
func handleIdleRound(fc *fakeConn, preEvents []string) bool {
	tag, rest, err := fc.readCommand()
	if err != nil {
		return false
	}
	up := strings.ToUpper(rest)
	if up == "LOGOUT" {
		_ = fc.write(tag + " OK bye\r\n")
		return false
	}
	if up != "IDLE" {
		_ = fc.write(tag + " BAD expected IDLE\r\n")
		return false
	}
	if fc.srv != nil {
		fc.srv.idleStarts.Add(1)
	}
	_ = fc.write("+ idling\r\n")
	for _, e := range preEvents {
		_ = fc.write(e)
	}
	_ = fc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := fc.r.ReadString('\n')
	_ = fc.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(line), "DONE") {
		return false
	}
	_ = fc.write(tag + " OK IDLE completed\r\n")
	if fc.srv != nil {
		fc.srv.idleRounds.Add(1)
	}
	return true
}

// idleBadContinuationScript replies to IDLE with NO instead of `+ idling`.
func idleBadContinuationScript(fc *fakeConn) {
	handleIdleBringUp(fc)
	for {
		tag, rest, err := fc.readCommand()
		if err != nil {
			return
		}
		up := strings.ToUpper(rest)
		switch {
		case up == "IDLE":
			_ = fc.write(tag + " NO can't idle right now\r\n")
		case up == "LOGOUT":
			_ = fc.write(tag + " OK bye\r\n")
			return
		default:
			_ = fc.write(tag + " BAD unknown\r\n")
		}
	}
}

// handleIdleBringUp runs the LOGIN/CAPABILITY/SELECT bring-up that every
// IDLE test needs before exercising IDLE itself.
func handleIdleBringUp(fc *fakeConn) {
	_ = fc.write("* OK fake imap ready\r\n")
	for {
		tag, rest, err := fc.readCommand()
		if err != nil {
			return
		}
		up := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(up, "LOGIN "):
			_ = fc.write(tag + " OK LOGIN completed\r\n")
		case up == "CAPABILITY":
			_ = fc.write("* CAPABILITY IMAP4rev1 IDLE LITERAL+\r\n")
			_ = fc.write(tag + " OK CAPABILITY completed\r\n")
		case strings.HasPrefix(up, "SELECT "):
			_ = fc.write("* 4 EXISTS\r\n")
			_ = fc.write("* OK [UIDNEXT 100] mailbox ready\r\n")
			_ = fc.write(tag + " OK [READ-WRITE] SELECT completed\r\n")
			return // bring-up done; caller handles next command
		}
	}
}

// handleNoIdleBringUp does the LOGIN/CAPABILITY/SELECT bring-up advertising
// CAPABILITY without IDLE — forces the session to choose its polling
// fallback path. UIDNEXT is included so sinceUID initializes cleanly.
func handleNoIdleBringUp(fc *fakeConn, uidNext uint32) {
	_ = fc.write("* OK fake imap ready\r\n")
	for {
		tag, rest, err := fc.readCommand()
		if err != nil {
			return
		}
		up := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(up, "LOGIN "):
			_ = fc.write(tag + " OK LOGIN completed\r\n")
		case up == "CAPABILITY":
			// No IDLE listed → session falls back to poll mode.
			_ = fc.write("* CAPABILITY IMAP4rev1 LITERAL+\r\n")
			_ = fc.write(tag + " OK CAPABILITY completed\r\n")
		case strings.HasPrefix(up, "SELECT "):
			_ = fc.write("* 4 EXISTS\r\n")
			if uidNext > 0 {
				_ = fc.write("* OK [UIDNEXT " + strconv.FormatUint(uint64(uidNext), 10) + "] mailbox ready\r\n")
			}
			_ = fc.write(tag + " OK [READ-WRITE] SELECT completed\r\n")
			return
		}
	}
}

// handleIdleBringUpWithUIDNext lets a test control the UIDNEXT value
// reported by SELECT. Useful for verifying sinceUID initialization edge
// cases (e.g., very high UIDNEXT means we skip the whole backlog).
func handleIdleBringUpWithUIDNext(fc *fakeConn, uidNext uint32) {
	_ = fc.write("* OK fake imap ready\r\n")
	for {
		tag, rest, err := fc.readCommand()
		if err != nil {
			return
		}
		up := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(up, "LOGIN "):
			_ = fc.write(tag + " OK LOGIN completed\r\n")
		case up == "CAPABILITY":
			_ = fc.write("* CAPABILITY IMAP4rev1 IDLE LITERAL+\r\n")
			_ = fc.write(tag + " OK CAPABILITY completed\r\n")
		case strings.HasPrefix(up, "SELECT "):
			_ = fc.write("* 4 EXISTS\r\n")
			if uidNext > 0 {
				_ = fc.write("* OK [UIDNEXT " + strconv.FormatUint(uint64(uidNext), 10) + "] mailbox ready\r\n")
			}
			_ = fc.write(tag + " OK [READ-WRITE] SELECT completed\r\n")
			return
		}
	}
}

// handleIdle services a single IDLE round: reads IDLE command, sends
// `+ idling`, emits any pre-canned untagged events, then waits for DONE.
// Returns after replying with the tagged IDLE OK. preEvents are written
// verbatim (each should already have a CRLF).
func handleIdle(fc *fakeConn, preEvents []string) {
	tag, rest, err := fc.readCommand()
	if err != nil {
		return
	}
	if !strings.EqualFold(rest, "IDLE") {
		_ = fc.write(tag + " BAD expected IDLE\r\n")
		return
	}
	_ = fc.write("+ idling\r\n")
	for _, e := range preEvents {
		_ = fc.write(e)
	}
	// Wait for DONE.
	for {
		// Reset deadline — DONE may take a while to arrive (caller-driven).
		_ = fc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := fc.r.ReadString('\n')
		_ = fc.conn.SetReadDeadline(time.Time{})
		if err != nil {
			return
		}
		if strings.EqualFold(strings.TrimSpace(line), "DONE") {
			_ = fc.write(tag + " OK IDLE completed\r\n")
			return
		}
	}
}

// noIdleScript advertises IMAP4rev1 only (no IDLE) so Capable("IDLE") is false.
func noIdleScript(fc *fakeConn) {
	_ = fc.write("* OK fake imap ready\r\n")
	for {
		tag, rest, err := fc.readCommand()
		if err != nil {
			return
		}
		up := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(up, "LOGIN "):
			_ = fc.write(tag + " OK LOGIN completed\r\n")
		case up == "CAPABILITY":
			_ = fc.write("* CAPABILITY IMAP4rev1\r\n")
			_ = fc.write(tag + " OK CAPABILITY completed\r\n")
		case strings.HasPrefix(up, "SELECT "):
			_ = fc.write("* OK [UIDNEXT 1] mailbox ready\r\n")
			_ = fc.write(tag + " OK SELECT completed\r\n")
		case up == "LOGOUT":
			_ = fc.write(tag + " OK bye\r\n")
			return
		default:
			_ = fc.write(tag + " BAD unknown\r\n")
		}
	}
}

// errorIfNoCommand returns an io.EOF-shaped error so tests can recognize
// post-disconnect read failures.
var errClosed = fmt.Errorf("connection closed")
