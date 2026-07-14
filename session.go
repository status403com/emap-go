package emap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// IdleRoundDuration is how long a single IDLE issuance is allowed to sit
// before the loop voluntarily re-issues it. IMAP servers cut at ~29 minutes
// (RFC 2177); 25 stays conservatively under that and matches what most
// production clients use. Var (not const) so tests can shorten it.
var IdleRoundDuration = 25 * time.Minute

// DefaultPollInterval is the default polling cadence when IDLE is
// unavailable or ForcePolling is set. Override per-Manager via
// Manager.PollInterval.
const DefaultPollInterval = 3 * time.Second

// Reconnect backoff parameters. When the IDLE loop hits a transient conn
// error, it retries with exponential backoff starting at ReconnectInitialBackoff
// and doubling each attempt, capped at ReconnectMaxBackoff. Tests shorten
// these to avoid second-scale waits.
var (
	ReconnectInitialBackoff = 1 * time.Second
	ReconnectMaxBackoff     = 60 * time.Second
)

// Filter narrows which messages a Subscription receives. Zero-value Filter
// matches every message that lands in the inbox during the subscription's
// lifetime — backlog protection lives at the IMAP layer (sinceUID), not
// here, so a zero Filter is safe even on an inbox with a million messages.
type Filter struct {
	// To matches the To: header verbatim (case-insensitive). Empty = no filter.
	// Catch-all flows set this to the per-task email; the manager indexes
	// subscriptions by this field for O(1) dispatch on arrival.
	To string
	// FromContains restricts to messages whose From: header contains this
	// substring (case-insensitive). Useful for "from no-reply@<vendor>" gating.
	FromContains string
	// Since, if non-zero, drops messages whose INTERNALDATE is strictly
	// earlier than this. Optional — leave zero to let sinceUID handle
	// backlog. Use this only for explicit time-based filtering (e.g. "ignore
	// any verification mail older than the moment I clicked send").
	Since time.Time
	// MaxAge, if non-zero, drops messages whose INTERNALDATE is older than
	// now-MaxAge at dispatch time. Useful as a sanity gate ("verification
	// mail is meaningless after 5 minutes"). Zero disables.
	MaxAge time.Duration
}

// Message is the parsed shape delivered to subscribers. Body is the decoded
// TEXT portion of the message — modules typically regex a verification URL
// or 6-digit code out of it.
type Message struct {
	UID     uint32
	To      string
	From    string
	Subject string
	Date    time.Time
	Body    string
}

// Subscription is a per-task handle on a credential session. Ch yields
// messages matching the Filter for the lifetime of the Subscription. Callers
// MUST Close when done; the channel closes on Close OR on session teardown.
type Subscription struct {
	Ch     <-chan Message
	ch     chan Message
	filter Filter
	closer func()
	once   sync.Once
}

// Close removes this subscription from its session and decrements the
// session's refcount. Safe to call multiple times.
func (s *Subscription) Close() {
	s.once.Do(s.closer)
}

// Session state values. Compared/stored via sync/atomic on session.state.
// Intermediate transitions during connect (greeted → authed → selected) are
// local — externally a session is just New, Active, or Closed.
const (
	sessionStateNew    int32 = 0
	sessionStateActive int32 = 1
	sessionStateClosed int32 = 2
)

// dialFunc lets tests substitute the real network dial with an in-memory
// fake. nil means use the real dialer.
type dialFunc func(host string, port int, useTLS bool) (*imapConn, error)

// session is the singleton per credential. Holds one persistent imapConn and
// the registry of subscribers reading off it.
type session struct {
	cred    Credential
	manager *Manager
	key     credKey

	// refs is the live Subscription count. Accessed via sync/atomic and as a
	// guard inside manager.mu — see Manager.Subscribe / releaseSub.
	refs int32

	// state is one of sessionStateNew / Active / Closed. Atomic so probes can
	// peek without taking a mutex; transitions to Closed are one-shot.
	state atomic.Int32

	// lingerTimer schedules teardown after the last unsubscribe. Manipulated
	// only under manager.mu.
	lingerTimer *time.Timer

	subsMu sync.Mutex
	subs   map[*Subscription]struct{}
	// subsByTo is a To-filter → subs index. The empty key holds wildcard subs
	// (Filter.To == ""), so dispatch is two O(1) lookups regardless of how
	// many subscribers a catch-all has. Keys are lowercased.
	subsByTo map[string][]*Subscription

	// connMu guards conn + selectedFolder during connect/reconnect/disconnect.
	// Held briefly; long reads happen on the conn directly.
	connMu         sync.Mutex
	conn           *imapConn
	selectedFolder string // remembered so reconnect can re-SELECT transparently

	// IDLE loop state. idleCtx is canceled in disconnect to stop the loop;
	// idleDone closes when the loop goroutine has fully exited so callers
	// can join before tearing down the conn. nil when the loop isn't running
	// (server didn't advertise IDLE, or disconnect already ran).
	idleCtx    context.Context
	idleCancel context.CancelFunc
	idleDone   chan struct{}

	// pendingFetch is set by the IDLE handler on every EXISTS untagged
	// response. The loop drains it between IDLE rounds; Phase 2C turns the
	// signal into an actual UID FETCH + dispatch.
	pendingFetch atomic.Bool

	// sinceUID is the highest UID we have already FETCH'd (or skipped at
	// startup via UIDNEXT - 1). FETCH only requests UIDs strictly greater
	// than this, so an inbox with a million backlog messages costs the same
	// as an empty one at connect time — we never download what we don't
	// need. Updated atomically after each FETCH; survives reconnects so
	// mail that arrives during a disconnect window is still picked up on
	// resume.
	sinceUID atomic.Uint32

	// sinceUIDInitialized guards against re-initializing sinceUID on
	// reconnect. The first successful connect sets it from UIDNEXT;
	// subsequent reconnects keep the watermark.
	sinceUIDInitialized atomic.Bool

	// idleRoundCancel is set on each IDLE round so the handler can force an
	// early exit from IDLE when EXISTS arrives (so we can FETCH, then re-IDLE).
	// Guarded by idleMu so the handler doesn't race with round setup.
	idleMu          sync.Mutex
	idleRoundCancel context.CancelFunc

	// dial is overridable from tests. nil = use the real dialIMAPConn.
	dial dialFunc
}

func newSession(c Credential, m *Manager, k credKey) *session {
	return &session{
		cred:     c,
		manager:  m,
		key:      k,
		subs:     make(map[*Subscription]struct{}),
		subsByTo: make(map[string][]*Subscription),
		dial:     m.dial,
	}
}

// toKey normalizes a recipient string for matching. Whitespace + case are
// stripped so "Alice@Example.com" matches "alice@example.com".
func toKey(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

// inboxFolder is the only mailbox the manager monitors. Catch-all and
// confirmation flows both land in INBOX; we don't venture into custom
// mailboxes (no MOVE / Spam handling yet).
const inboxFolder = "INBOX"

// connect runs the full bring-up sequence: dial → LOGIN → CAPABILITY →
// SELECT INBOX. Leaves the session in sessionStateActive on success and
// starts the IDLE loop.
func (s *session) connect() error {
	if err := s.establish(false); err != nil {
		return err
	}
	s.startIdleLoop()
	return nil
}

// establish performs the actual dial + auth + SELECT. Shared between the
// initial connect path (called from Manager.Subscribe) and the in-loop
// reconnect path (called from runIdleLoop, which is its own goroutine and
// must NOT touch the IDLE loop lifecycle).
//
// isReconnect=true: close any stale conn first, restore the previously
// selected folder if it wasn't INBOX. isReconnect=false: assume no
// pre-existing conn.
func (s *session) establish(isReconnect bool) error {
	var prevFolder string
	if isReconnect {
		s.connMu.Lock()
		prevFolder = s.selectedFolder
		old := s.conn
		s.conn = nil
		s.connMu.Unlock()
		if old != nil {
			_ = old.Close()
		}
	}

	dial := s.dial
	if dial == nil {
		dial = dialIMAPConn
	}

	c, err := dial(s.cred.Host, s.cred.Port, s.cred.UseTLS)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	if err := c.login(s.cred.Email, s.cred.Password); err != nil {
		_ = c.Close()
		return fmt.Errorf("login: %w", err)
	}
	if err := c.capability(); err != nil {
		_ = c.Close()
		return fmt.Errorf("capability: %w", err)
	}
	if err := c.selectFolder(inboxFolder); err != nil {
		_ = c.Close()
		return fmt.Errorf("select %s: %w", inboxFolder, err)
	}

	folder := inboxFolder
	if isReconnect && prevFolder != "" && prevFolder != inboxFolder {
		if err := c.selectFolder(prevFolder); err != nil {
			_ = c.Close()
			return fmt.Errorf("restore folder %s: %w", prevFolder, err)
		}
		folder = prevFolder
	}

	s.connMu.Lock()
	s.conn = c
	s.selectedFolder = folder
	s.connMu.Unlock()
	s.state.Store(sessionStateActive)
	s.initSinceUID(c)
	return nil
}

// initSinceUID sets the backlog skip-watermark from UIDNEXT. Called on every
// connect, but only takes effect once — reconnects keep the existing
// watermark so mail that arrived during the disconnect window is still
// fetched on resume.
//
// CRITICAL: we only mark initialized=true when UIDNEXT was actually parsed
// from the SELECT response. A non-conformant server that omits UIDNEXT
// leaves us with c.uidNext == 0; in that case we keep initialized=false so
// fetchNewMessages refuses to issue `UID FETCH 1:*` and accidentally pull
// the entire backlog. Without this guard, connecting to an inbox with a
// million stored messages would attempt to download all of them.
func (s *session) initSinceUID(c *imapConn) {
	if s.sinceUIDInitialized.Load() {
		return
	}
	if c.uidNext > 0 {
		s.sinceUID.Store(c.uidNext - 1)
		s.sinceUIDInitialized.Store(true)
	}
}

// startIdleLoop spawns the per-session watch goroutine. Picks the IDLE path
// when the server advertised it via CAPABILITY; otherwise falls back to a
// polling loop on PollInterval. The chosen path is fixed for the goroutine's
// lifetime — capability re-evaluation happens at the next startIdleLoop()
// call (i.e. after stopIdleLoop has joined). idleDone/idleCtx/idleCancel
// fields are reused for both paths since they're per-loop, not per-mode.
//
// Safe to call repeatedly; a running loop is detected by idleDone != nil.
func (s *session) startIdleLoop() {
	s.connMu.Lock()
	c := s.conn
	s.connMu.Unlock()
	if c == nil {
		return
	}
	if s.idleDone != nil {
		return // already running
	}
	s.idleCtx, s.idleCancel = context.WithCancel(context.Background())
	s.idleDone = make(chan struct{})
	if c.Capable("IDLE") && !s.manager.ForcePolling {
		go s.runIdleLoop(s.idleCtx, c)
	} else {
		go s.runPollLoop(s.idleCtx, c)
	}
}

// stopIdleLoop cancels the watch context (which propagates to the round ctx
// for IDLE; the next select for poll) and waits for the loop goroutine to
// exit. Safe to call when no loop is running.
func (s *session) stopIdleLoop() {
	if s.idleDone == nil {
		return
	}
	s.idleCancel()
	<-s.idleDone
	s.idleCtx = nil
	s.idleCancel = nil
	s.idleDone = nil
}

// runPollLoop is the fallback watch goroutine for servers that don't support
// IDLE. Issues UID FETCH on a fixed cadence (PollInterval). Same backoff and
// reconnect semantics as runIdleLoop: transport errors trigger reconnect with
// exponential backoff; protocol errors from FETCH would also reconnect (we
// don't yet distinguish — a FETCH BAD is rare and reconnecting is the
// pragmatic answer).
//
// Doesn't use pendingFetch because polling drives every cycle directly.
func (s *session) runPollLoop(parentCtx context.Context, c *imapConn) {
	defer close(s.idleDone)

	interval := s.manager.PollInterval
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for parentCtx.Err() == nil {
		if err := s.fetchNewMessages(c); err != nil && parentCtx.Err() == nil {
			newC, ok := s.reconnectWithBackoff(parentCtx)
			if !ok {
				return
			}
			c = newC
			continue // retry immediately on the fresh conn
		}

		select {
		case <-ticker.C:
		case <-parentCtx.Done():
			return
		}
	}
}

// runIdleLoop is the body of the IDLE goroutine. Each iteration runs one
// IDLE round bounded by IdleRoundDuration; arrivals (EXISTS) cause an early
// exit via idleRoundCancel so we can FETCH between rounds.
//
// On conn error the loop exits — Phase 2D wires the reconnect path. Caller
// (disconnect or reconnect) is responsible for starting a fresh loop after
// re-establishing the connection.
func (s *session) runIdleLoop(parentCtx context.Context, c *imapConn) {
	defer close(s.idleDone)

	for parentCtx.Err() == nil {
		roundCtx, roundCancel := context.WithTimeout(parentCtx, IdleRoundDuration)

		s.idleMu.Lock()
		s.idleRoundCancel = roundCancel
		s.idleMu.Unlock()

		err := c.idle(roundCtx, s.handleIdleLine)

		s.idleMu.Lock()
		s.idleRoundCancel = nil
		s.idleMu.Unlock()
		roundCancel()

		if err != nil && parentCtx.Err() == nil {
			// Distinguish "the server doesn't want to IDLE" from a transport
			// failure. A protocol-level rejection (BAD/NO) won't be fixed by
			// reconnecting — a fresh conn would just hit the same response.
			// Exit cleanly instead of looping forever burning server slots.
			if errors.Is(err, errIDLERejected) {
				return
			}
			// Transport failure (server drop, network blip, BYE before
			// tagged response). Reconnect with exponential backoff and
			// continue on the fresh conn. If parentCtx cancels during
			// backoff (disconnect was called), exit cleanly.
			newC, ok := s.reconnectWithBackoff(parentCtx)
			if !ok {
				return
			}
			c = newC
			// After reconnect, force a FETCH so any mail that arrived
			// during the outage is delivered before we re-IDLE.
			s.pendingFetch.Store(true)
		}

		if s.pendingFetch.Swap(false) {
			s.handlePendingFetch(c)
		}
	}
}

// handleIdleLine is the per-untagged-line callback passed to conn.idle. Sets
// the pendingFetch flag on any EXISTS notification and cancels the current
// round so the loop can FETCH immediately rather than waiting up to 25 min.
//
// Runs on conn.idle's reader goroutine — MUST be non-blocking.
func (s *session) handleIdleLine(line string) {
	if !isExistsLine(line) {
		return
	}
	s.pendingFetch.Store(true)
	s.idleMu.Lock()
	cancel := s.idleRoundCancel
	s.idleMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// isExistsLine reports whether an untagged IDLE response is an EXISTS
// notification, e.g. "5 EXISTS". The caller has already stripped the leading
// "* " prefix.
func isExistsLine(line string) bool {
	// Format: "<seq> EXISTS"; minimum well-formed payload is "0 EXISTS".
	idx := strings.IndexByte(line, ' ')
	if idx <= 0 {
		return false
	}
	return strings.EqualFold(line[idx+1:], "EXISTS")
}

// handlePendingFetch runs UID FETCH for mail above sinceUID, parses each
// response, and dispatches matching ones to subscribers. Errors are
// swallowed here — Phase 2D upgrades persistent failures into reconnect
// attempts.
func (s *session) handlePendingFetch(c *imapConn) {
	_ = s.fetchNewMessages(c)
}

// reconnect is the external reconnect entry point: stops the IDLE loop,
// dials fresh, restores the selected folder, then restarts the loop. Use
// from outside the IDLE goroutine; the loop's own reconnect path uses
// reconnectInPlace which doesn't touch the loop's lifecycle.
func (s *session) reconnect() error {
	if s.state.Load() == sessionStateClosed {
		return fmt.Errorf("session closed")
	}
	s.stopIdleLoop()
	if err := s.establish(true); err != nil {
		return err
	}
	s.startIdleLoop()
	return nil
}

// reconnectInPlace re-establishes the conn without stopping or restarting
// the IDLE loop. Designed for use FROM the IDLE loop itself — the loop
// continues running on the returned new conn. Returns the fresh *imapConn
// so the caller can update its local reference.
func (s *session) reconnectInPlace() (*imapConn, error) {
	if s.state.Load() == sessionStateClosed {
		return nil, fmt.Errorf("session closed")
	}
	if err := s.establish(true); err != nil {
		return nil, err
	}
	s.connMu.Lock()
	c := s.conn
	s.connMu.Unlock()
	return c, nil
}

// reconnectWithBackoff retries reconnectInPlace until success or the parent
// context is canceled. Wait between attempts doubles starting at
// ReconnectInitialBackoff, capped at ReconnectMaxBackoff. Returns the new
// conn and true on success; nil + false if the caller's ctx canceled first
// (loop should exit).
func (s *session) reconnectWithBackoff(ctx context.Context) (*imapConn, bool) {
	delay := ReconnectInitialBackoff
	for ctx.Err() == nil {
		if c, err := s.reconnectInPlace(); err == nil {
			return c, true
		}
		// Wait with cancellation. timer.Stop avoids the timer leaking when
		// ctx fires first.
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, false
		}
		delay *= 2
		if delay > ReconnectMaxBackoff {
			delay = ReconnectMaxBackoff
		}
	}
	return nil, false
}

// disconnect closes the underlying conn (best-effort LOGOUT) and forcibly
// closes all subscriber channels. Idempotent; safe from teardown/Shutdown.
//
// Order matters: stop the IDLE loop first so LOGOUT doesn't deadlock on
// execMu (idle holds it for the full IDLE duration). After the loop joins,
// the conn is free for the LOGOUT round-trip before Close.
func (s *session) disconnect() {
	s.state.Store(sessionStateClosed)

	s.stopIdleLoop()

	s.connMu.Lock()
	c := s.conn
	s.conn = nil
	s.connMu.Unlock()
	if c != nil {
		c.logout()
		_ = c.Close()
	}

	s.subsMu.Lock()
	for sub := range s.subs {
		close(sub.ch)
		delete(s.subs, sub)
	}
	for k := range s.subsByTo {
		delete(s.subsByTo, k)
	}
	s.subsMu.Unlock()
}

// subscribe registers a Subscription against this session and returns it.
// Caller must have already incremented s.refs under manager.mu.
//
// Filter.Since is NOT auto-set. Backlog protection is handled at the IMAP
// layer via sinceUID (initialized from UIDNEXT) — we never FETCH messages
// the session already saw. Auto-setting Since to time.Now() races at
// sub-second granularity against INTERNALDATE's second-precision and would
// silently drop legitimate mail.
func (s *session) subscribe(f Filter) *Subscription {
	ch := make(chan Message, subscriberBuffer)
	sub := &Subscription{Ch: ch, ch: ch, filter: f}
	sub.closer = func() { s.unsubscribe(sub) }

	tk := toKey(f.To) // "" for wildcards

	s.subsMu.Lock()
	s.subs[sub] = struct{}{}
	s.subsByTo[tk] = append(s.subsByTo[tk], sub)
	s.subsMu.Unlock()

	return sub
}

// unsubscribe removes a subscription from the session, closes its channel,
// and notifies the manager so it can refcount down.
func (s *session) unsubscribe(sub *Subscription) {
	s.subsMu.Lock()
	if _, ok := s.subs[sub]; !ok {
		s.subsMu.Unlock()
		return
	}
	delete(s.subs, sub)
	tk := toKey(sub.filter.To)
	if list := s.subsByTo[tk]; len(list) > 0 {
		// Swap-remove; order doesn't matter for the dispatch fanout.
		for i, x := range list {
			if x == sub {
				list[i] = list[len(list)-1]
				s.subsByTo[tk] = list[:len(list)-1]
				break
			}
		}
		if len(s.subsByTo[tk]) == 0 {
			delete(s.subsByTo, tk)
		}
	}
	close(sub.ch)
	s.subsMu.Unlock()
	s.manager.releaseSub(s)
}

// dispatch fans a message out to every subscriber whose Filter matches. Held
// under subsMu so a concurrent unsubscribe can't close a channel mid-send.
// Sends are non-blocking with drop-oldest semantics — a hung consumer can't
// pin memory or stall the dispatcher.
//
// Phase 1C: callers inject via injectMessage (test-only). Phase 2 wires this
// to IDLE EXISTS arrivals.
func (s *session) dispatch(msg Message) {
	tk := toKey(msg.To)
	now := time.Now()

	s.subsMu.Lock()
	defer s.subsMu.Unlock()

	// Two O(1) lookups: exact-To match and wildcards.
	candidates := s.subsByTo[tk]
	if tk != "" {
		candidates = append(candidates, s.subsByTo[""]...)
	}
	for _, sub := range candidates {
		if !matchFilter(sub.filter, msg, now) {
			continue
		}
		deliverOrDropOldest(sub.ch, msg)
	}
}

// matchFilter returns true when msg satisfies the secondary filter fields.
// The To dimension is already enforced by the index lookup; this only checks
// From / Since / MaxAge.
func matchFilter(f Filter, msg Message, now time.Time) bool {
	if f.FromContains != "" {
		if !strings.Contains(strings.ToLower(msg.From), strings.ToLower(f.FromContains)) {
			return false
		}
	}
	if !f.Since.IsZero() && !msg.Date.IsZero() && msg.Date.Before(f.Since) {
		return false
	}
	if f.MaxAge > 0 && !msg.Date.IsZero() && now.Sub(msg.Date) > f.MaxAge {
		return false
	}
	return true
}

// deliverOrDropOldest pushes msg onto ch. If ch is full, the oldest queued
// message is discarded to make room — this keeps a slow consumer from pinning
// the entire IMAP pipeline. Caller must hold subsMu so ch can't be closed
// concurrently (closed-channel send panics).
func deliverOrDropOldest(ch chan Message, msg Message) {
	select {
	case ch <- msg:
		return
	default:
	}
	// Drop oldest then retry. Both ops are non-blocking — if ch was drained
	// in the meantime, the receive falls through and the send succeeds.
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- msg:
	default:
		// Channel raced full again; drop the new message silently. This is
		// the intended bound under sustained pressure.
	}
}

// subscriberBuffer is the per-subscription channel capacity. Sized small so a
// hung consumer can't pin large amounts of body memory; dispatch is
// non-blocking and drops oldest on a full channel (Phase 1C wires the drop).
const subscriberBuffer = 8
