package emap

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultLinger is the grace period a session stays connected after its last
// subscriber unsubscribes. A new Subscribe within this window reuses the
// existing TCP/auth state instead of paying for a fresh LOGIN round-trip.
const DefaultLinger = 60 * time.Second

// Manager pools one *session per unique IMAP credential, ref-counted by
// active subscriptions. Many tasks reading the same catch-all inbox share a
// single connection rather than each opening their own — critical at the
// 2000-task scale where every IMAP server enforces a low concurrent-conn cap
// per account (Gmail caps at 15).
type Manager struct {
	mu       sync.Mutex
	sessions map[credKey]*session
	linger   time.Duration
	// ForcePolling disables IDLE even when the server supports it, falling
	// back to polling at PollInterval. Useful when the server's IDLE
	// notifications are unreliably delayed (e.g. Gmail).
	ForcePolling bool
	// PollInterval is how long the polling fallback waits between FETCH
	// cycles. Per credential, not per subscriber. Zero uses
	// DefaultPollInterval (3s).
	PollInterval time.Duration
	// dial overrides the real network dialer. nil = use dialIMAPConn. Set
	// only by tests via withDial.
	dial dialFunc
}

// withDial swaps in a custom dialer. Test-only; never called from prod code.
func (m *Manager) withDial(d dialFunc) *Manager {
	m.dial = d
	return m
}

// NewManager constructs a Manager. Pass 0 for DefaultLinger.
func NewManager(linger time.Duration) *Manager {
	if linger <= 0 {
		linger = DefaultLinger
	}
	return &Manager{
		sessions: make(map[credKey]*session),
		linger:   linger,
	}
}

// credKey identifies a credential by the tuple needed to connect. Email is
// lowercased so case differences from user input don't fork into separate
// sessions for the same mailbox.
type credKey struct {
	Host  string
	Port  int
	Email string
}

func keyFor(c Credential) credKey {
	return credKey{
		Host:  strings.ToLower(strings.TrimSpace(c.Host)),
		Port:  c.Port,
		Email: strings.ToLower(strings.TrimSpace(c.Email)),
	}
}

// Subscribe attaches to the credential's session, creating one if necessary.
// The returned Subscription MUST be Close()d when the caller is done so the
// session can tear down once its last subscriber leaves.
//
// Phase 1A: connect() is a stub; the returned Subscription's Ch never delivers.
// Real fetch dispatch arrives in Phase 1C.
func (m *Manager) Subscribe(c Credential, f Filter) (*Subscription, error) {
	k := keyFor(c)

	m.mu.Lock()
	s, ok := m.sessions[k]
	if !ok {
		s = newSession(c, m, k)
		if err := s.connect(); err != nil {
			m.mu.Unlock()
			return nil, err
		}
		m.sessions[k] = s
	}
	// Revive a session caught mid-linger: cancel the scheduled teardown so
	// we don't lose this connection seconds after picking it back up.
	if s.lingerTimer != nil {
		s.lingerTimer.Stop()
		s.lingerTimer = nil
	}
	atomic.AddInt32(&s.refs, 1)
	m.mu.Unlock()

	return s.subscribe(f), nil
}

// releaseSub is invoked by Subscription.Close. Decrements the session's
// refcount and schedules a linger teardown once it hits zero. A new Subscribe
// before linger expires cancels the timer.
func (m *Manager) releaseSub(s *session) {
	if atomic.AddInt32(&s.refs, -1) > 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check under the lock — a Subscribe may have raced past us.
	if atomic.LoadInt32(&s.refs) != 0 || s.lingerTimer != nil {
		return
	}
	s.lingerTimer = time.AfterFunc(m.linger, func() { m.teardown(s) })
}

// teardown is the linger timer's fn. Bails if the session was revived
// (refs > 0) or the timer was already canceled (lingerTimer == nil).
func (m *Manager) teardown(s *session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.lingerTimer == nil || atomic.LoadInt32(&s.refs) > 0 {
		return
	}
	s.lingerTimer = nil
	delete(m.sessions, s.key)
	s.disconnect()
}

// Shutdown disconnects every active session immediately. Called at app exit
// so we don't leak file descriptors or goroutines on a clean quit.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	live := make([]*session, 0, len(m.sessions))
	for k, s := range m.sessions {
		if s.lingerTimer != nil {
			s.lingerTimer.Stop()
			s.lingerTimer = nil
		}
		live = append(live, s)
		delete(m.sessions, k)
	}
	m.mu.Unlock()
	for _, s := range live {
		s.disconnect()
	}
}

// activeSessions reports the number of live sessions. Test-only.
func (m *Manager) activeSessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}
