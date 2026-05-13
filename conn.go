package emap

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// errIDLERejected indicates the server returned BAD/NO to IDLE or replied
// with anything other than the `+ idling` continuation. Distinct from
// transport errors: the connection is still healthy, the server just
// doesn't want to IDLE. The session loop checks for this and exits rather
// than reconnect (a fresh connection would hit the same rejection).
var errIDLERejected = errors.New("server rejected IDLE")

// Tunable timeouts for the long-lived imapConn. Vars (not consts) so tests
// can override without resorting to global mocks.
var (
	// ConnDialTimeout bounds TCP/TLS handshake.
	ConnDialTimeout = 10 * time.Second
	// ConnGreetingTimeout bounds reading the server greeting after dial.
	ConnGreetingTimeout = 10 * time.Second
	// ConnCommandTimeout bounds a single tagged command (LOGIN, SELECT,
	// CAPABILITY, etc). Zero = no per-command deadline. IDLE (Phase 2) is
	// special-cased — it would otherwise expire after 30s.
	ConnCommandTimeout = 30 * time.Second
)

// imapConn is a single long-lived IMAP connection. Commands are serialized
// via execMu; one in flight at a time. Owns a sized bufio.Reader so small
// lines don't churn the heap.
//
// Caller is responsible for invoking authenticate / selectFolder / capability
// after dial. See session.go for the full bring-up sequence.
type imapConn struct {
	conn   net.Conn
	r      *bufio.Reader
	execMu sync.Mutex
	tagCtr int

	capsMu sync.RWMutex
	caps   map[string]struct{}

	// uidNext is the next UID the server will assign in the currently
	// selected mailbox, captured from `* OK [UIDNEXT N]` after SELECT.
	// Session uses this to initialize sinceUID so we don't FETCH backlog
	// mail that pre-existed the subscription.
	uidNext uint32
}

// dialIMAPConn opens TCP (or TLS) to host:port, reads the server greeting,
// and returns a ready connection. Caller still has to authenticate.
func dialIMAPConn(host string, port int, useTLS bool) (*imapConn, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	dialer := &net.Dialer{Timeout: ConnDialTimeout}

	var raw net.Conn
	var err error
	if useTLS {
		raw, err = tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{ServerName: host})
	} else {
		raw, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return wrapConnAndReadGreeting(raw)
}

// wrapConnAndReadGreeting builds an imapConn around an already-established
// net.Conn and reads the server greeting. Shared between the real dial path
// and test fakes that hand in net.Pipe halves.
func wrapConnAndReadGreeting(raw net.Conn) (*imapConn, error) {
	c := &imapConn{
		conn: raw,
		r:    bufio.NewReaderSize(raw, 8192),
		caps: make(map[string]struct{}),
	}
	_ = raw.SetReadDeadline(time.Now().Add(ConnGreetingTimeout))
	line, err := c.readResponseLine()
	_ = raw.SetReadDeadline(time.Time{})
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("read greeting: %w", err)
	}
	if !strings.Contains(line, "OK") {
		_ = raw.Close()
		return nil, fmt.Errorf("unexpected greeting: %s", line)
	}
	return c, nil
}

func (c *imapConn) nextTag() string {
	c.tagCtr++
	return "A" + strconv.Itoa(c.tagCtr)
}

// readResponseLine reads one logical IMAP response line. Inlines any
// `{N}` literal continuations so the caller sees a single string per
// untagged/tagged response regardless of literal use.
func (c *imapConn) readResponseLine() (string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	for {
		n, ok := parseLiteralSuffix(line)
		if !ok {
			return line, nil
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return "", fmt.Errorf("read literal (%d bytes): %w", n, err)
		}
		rest, err := c.r.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("read literal tail: %w", err)
		}
		line = line + string(buf) + strings.TrimRight(rest, "\r\n")
	}
}

// exec sends one tagged command and reads until the matching tagged response.
// Serialized: only one command in flight at a time per connection.
func (c *imapConn) exec(cmd string) (string, []string, error) {
	c.execMu.Lock()
	defer c.execMu.Unlock()

	tag := c.nextTag()
	if ConnCommandTimeout > 0 {
		_ = c.conn.SetDeadline(time.Now().Add(ConnCommandTimeout))
		defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	}

	if _, err := c.conn.Write([]byte(tag + " " + cmd + "\r\n")); err != nil {
		return tag, nil, fmt.Errorf("write %q: %w", cmd, err)
	}

	var lines []string
	for {
		line, err := c.readResponseLine()
		if err != nil {
			return tag, lines, fmt.Errorf("read response to %q: %w", cmd, err)
		}
		lines = append(lines, line)
		if strings.HasPrefix(line, tag+" ") {
			return tag, lines, nil
		}
	}
}

// login authenticates the connection. Quotes in credentials are escaped per
// RFC 3501 §4.3 quoted-string rules.
func (c *imapConn) login(email, password string) error {
	cmd := fmt.Sprintf(`LOGIN "%s" "%s"`, escapeQ(email), escapeQ(password))
	tag, lines, err := c.exec(cmd)
	if err != nil {
		return err
	}
	return expectOK(tag, lines, "LOGIN")
}

// selectFolder issues SELECT for a mailbox. Caller is expected to remember
// which folder is selected so reconnect can re-select transparently. Also
// captures UIDNEXT from the response so the session can initialize sinceUID
// and skip backlog mail.
func (c *imapConn) selectFolder(folder string) error {
	tag, lines, err := c.exec("SELECT " + folder)
	if err != nil {
		return err
	}
	if err := expectOK(tag, lines, "SELECT"); err != nil {
		return err
	}
	c.uidNext = 0
	for _, l := range lines {
		// Format: "* OK [UIDNEXT <N>] mailbox ready"
		const prefix = "* OK [UIDNEXT "
		if !strings.HasPrefix(l, prefix) {
			continue
		}
		rest := l[len(prefix):]
		end := strings.IndexByte(rest, ']')
		if end < 0 {
			continue
		}
		if n, err := strconv.ParseUint(strings.TrimSpace(rest[:end]), 10, 32); err == nil {
			c.uidNext = uint32(n)
		}
	}
	return nil
}

// capability runs CAPABILITY and populates the capability set. The set is
// case-normalized to upper for cheap lookup via Capable().
func (c *imapConn) capability() error {
	tag, lines, err := c.exec("CAPABILITY")
	if err != nil {
		return err
	}
	if err := expectOK(tag, lines, "CAPABILITY"); err != nil {
		return err
	}
	c.capsMu.Lock()
	defer c.capsMu.Unlock()
	// Wipe any prior caps so reconnect picks up server changes cleanly.
	c.caps = make(map[string]struct{})
	for _, l := range lines {
		if !strings.HasPrefix(l, "* CAPABILITY") {
			continue
		}
		for _, f := range strings.Fields(l[len("* CAPABILITY"):]) {
			c.caps[strings.ToUpper(f)] = struct{}{}
		}
	}
	return nil
}

// Capable reports whether the server advertised a capability. Names are
// case-insensitive; pass IDLE, LITERAL+, UIDPLUS, etc.
func (c *imapConn) Capable(name string) bool {
	c.capsMu.RLock()
	defer c.capsMu.RUnlock()
	_, ok := c.caps[strings.ToUpper(name)]
	return ok
}

// logout sends LOGOUT best-effort. Errors are intentionally swallowed — the
// caller is closing the conn regardless and a half-broken server shouldn't
// stall shutdown.
func (c *imapConn) logout() {
	_, _, _ = c.exec("LOGOUT")
}

// execFetch sends a FETCH-shaped command and returns one rawFetchResp per
// `* N FETCH (...)` line in the server response. FETCH responses inline
// literal blocks (`{N}\r\n<bytes>`) that may contain arbitrary bytes,
// including parens and quotes — readResponseLine can't safely concatenate
// them and re-parse later. We capture each literal separately and substitute
// `\x00L<idx>\x00` placeholders so a downstream regex parser can pick them
// out by index.
func (c *imapConn) execFetch(cmd string) ([]rawFetchResp, error) {
	c.execMu.Lock()
	defer c.execMu.Unlock()

	tag := c.nextTag()
	if ConnCommandTimeout > 0 {
		_ = c.conn.SetDeadline(time.Now().Add(ConnCommandTimeout))
		defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	}

	if _, err := c.conn.Write([]byte(tag + " " + cmd + "\r\n")); err != nil {
		return nil, fmt.Errorf("write %q: %w", cmd, err)
	}

	var responses []rawFetchResp
	for {
		line, literals, err := c.readFetchLine()
		if err != nil {
			return nil, fmt.Errorf("read FETCH response: %w", err)
		}
		if strings.HasPrefix(line, tag+" ") {
			if !strings.Contains(line, " OK") {
				return nil, fmt.Errorf("FETCH failed: %s", line)
			}
			return responses, nil
		}
		if strings.HasPrefix(line, "* ") {
			if r, ok := parseFetchResponse(line, literals); ok {
				responses = append(responses, r)
			}
		}
	}
}

// readFetchLine reads one logical FETCH response, preserving literal blocks
// separately so the parser can extract them by index. Returns the line with
// each `{N}` marker replaced by a `\x00L<idx>\x00` placeholder, plus the
// captured literals in order. Non-literal lines return literals=nil.
func (c *imapConn) readFetchLine() (string, [][]byte, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	var literals [][]byte
	for {
		n, ok := parseLiteralSuffix(line)
		if !ok {
			return line, literals, nil
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return "", nil, fmt.Errorf("read literal (%d bytes): %w", n, err)
		}
		literals = append(literals, buf)
		rest, err := c.r.ReadString('\n')
		if err != nil {
			return "", nil, fmt.Errorf("read literal tail: %w", err)
		}
		i := strings.LastIndexByte(line, '{')
		if i < 0 {
			return "", nil, fmt.Errorf("internal: literal suffix found but '{' missing")
		}
		line = line[:i] + "\x00L" + strconv.Itoa(len(literals)-1) + "\x00" + strings.TrimRight(rest, "\r\n")
	}
}

// idle issues the IMAP IDLE command and streams every untagged response
// (EXISTS / EXPUNGE / FETCH flag updates) to handler. Returns nil when the
// server completes IDLE cleanly (after our DONE) or an error on connection
// failure / unexpected server response.
//
// Cancelling ctx triggers a DONE write to the server, which is safe to do
// concurrently with the read loop per net.Conn's contract. handler runs on
// the read goroutine and MUST be non-blocking — any heavy work belongs
// behind a channel or goroutine on the caller's side.
//
// Holds execMu for the full duration. IMAP forbids any other command while
// IDLE is active, so serializing matches the protocol. Read deadlines on
// the underlying conn are explicitly cleared at entry and not restored —
// IDLE may legitimately sit silent for ~25 minutes between server pings.
func (c *imapConn) idle(ctx context.Context, handler func(line string)) error {
	c.execMu.Lock()
	defer c.execMu.Unlock()

	// IDLE can outrun ConnCommandTimeout by orders of magnitude. Clear any
	// inherited deadline (a prior exec would have already cleared it, but
	// be defensive) and don't set a new one — ctx is the only timeout.
	_ = c.conn.SetDeadline(time.Time{})

	tag := c.nextTag()
	if _, err := c.conn.Write([]byte(tag + " IDLE\r\n")); err != nil {
		return fmt.Errorf("write IDLE: %w", err)
	}

	// Server must reply with `+ idling` (a continuation) before we can
	// safely treat subsequent responses as IDLE events.
	line, err := c.readResponseLine()
	if err != nil {
		return fmt.Errorf("read IDLE continuation: %w", err)
	}
	if !strings.HasPrefix(line, "+") {
		return fmt.Errorf("%w: expected `+ idling`, got: %s", errIDLERejected, line)
	}

	// Watcher: writes DONE the moment ctx is canceled. net.Conn allows
	// concurrent Read + Write across goroutines, so this is race-clean.
	// done channel signals "the function is returning; you can stop watching."
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			// Best-effort. If the server already terminated us, the write
			// fails and we discard the error.
			_, _ = c.conn.Write([]byte("DONE\r\n"))
		case <-done:
		}
	}()

	for {
		line, err := c.readResponseLine()
		if err != nil {
			return fmt.Errorf("read during IDLE: %w", err)
		}
		if strings.HasPrefix(line, tag+" ") {
			// Tagged response after our DONE — IDLE wrapped up.
			if strings.Contains(line, " OK") {
				return nil
			}
			return fmt.Errorf("%w: terminated with %s", errIDLERejected, line)
		}
		if handler != nil && strings.HasPrefix(line, "* ") {
			handler(line[2:])
		}
	}
}

// Close releases the underlying connection. Idempotent.
func (c *imapConn) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}
