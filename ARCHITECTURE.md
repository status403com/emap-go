# emap-go Architecture

## Design goals

1. **Fanout, not 1:1.** Many subscribers, few inboxes — the opposite of
   what most IMAP libraries assume.
2. **Memory and connection cost is bounded by load, not inbox size.** A
   million-message backlog must cost the same as an empty inbox at startup.
3. **Minimal surface area for leaks.** Hand-rolled wire protocol, stdlib
   only. No transitive deps that we can't audit.
4. **Crash-only semantics.** Transport errors get reconnect + backoff.
   Protocol errors fail fast. No silent retries that look like progress.

## Layers

```
┌──────────────────────────────────────────────────────────────────┐
│              user code (e.g. a task scheduler)                   │
│                                                                  │
│   Subscribe(cred, Filter{To: "verify+task1@..."})                │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────┐
│                          Manager                                 │
│                                                                  │
│   sessions: map[credKey]*session                                 │
│   - key: (host, port, lowercased email)                          │
│   - refcounts subscriptions per session                          │
│   - linger timer reuses warm sessions across rapid churn         │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────┐
│                       session (per credential)                   │
│                                                                  │
│   - state machine: New → Active → Closed                         │
│   - subs map + subsByTo index for O(1) dispatch by To address    │
│   - sinceUID watermark (init from UIDNEXT, survives reconnects)  │
│   - IDLE or poll loop goroutine                                  │
│                                                                  │
│   ┌──────────────────────────────────────────────────────────┐   │
│   │  watch loop                                              │   │
│   │  - if Capable(IDLE): IDLE round → on EXISTS, force FETCH │   │
│   │  - else:             ticker → FETCH every PollInterval   │   │
│   │  - on transport err: reconnect with exponential backoff  │   │
│   │  - on protocol err:  exit cleanly (don't loop on BAD/NO) │   │
│   └──────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────┐
│                  imapConn (single TCP/TLS conn)                  │
│                                                                  │
│   - bufio.Reader (sized 8 KB)                                    │
│   - execMu serializes commands                                   │
│   - readResponseLine inlines literal blocks for normal cmds      │
│   - readFetchLine captures literals separately for FETCH parsing │
│   - idle() supports concurrent DONE write while reader is active │
└──────────────────────────────────────────────────────────────────┘
```

## Pooling model

```
       Task 1 (verify+1@…)
       Task 2 (verify+2@…)         ┌────────────────────────┐
       Task 3 (verify+3@…)  ─────► │ session for catchall   │ ─── one TCP conn
       Task 4 (verify+4@…)         │ refs=2000              │     to imap.example.com
            …                      └────────────────────────┘
       Task 2000 (verify+2000@…)
```

- Credential identity for the pool key is `(host, port, lowercased email)`.
  Password is excluded — rotating the password reuses the same session entry.
- Each `Manager.Subscribe` is a refcount increment, not a new connection.
- `Subscription.Close` is a refcount decrement. When refs hits zero, a 60-second
  linger timer starts. A new `Subscribe` within that window cancels the timer
  and reuses the still-warm connection (no fresh LOGIN round-trip).
- Linger fires → session.disconnect() → LOGOUT + close conn + remove from map.

## UIDNEXT-based backlog skip

The first guarantee emap-go makes: **we never download backlog mail.**

```
SELECT INBOX
* 1234 EXISTS
* OK [UIDNEXT 5000] mailbox ready    ←  server tells us next UID
A1 OK [READ-WRITE] SELECT completed

session.sinceUID = 5000 - 1 = 4999    ←  treat everything up to 4999 as seen

(no FETCH issued)

later: mail arrives, UID 5000
* 1235 EXISTS                         ←  IDLE notification
→ session loop: handlePendingFetch
→ UID FETCH 5000:* (UID INTERNALDATE BODY.PEEK[...] BODY.PEEK[TEXT])
→ server returns just UID 5000
→ session.sinceUID = 5000
→ dispatch to matching subscribers
```

If a server omits UIDNEXT from the SELECT response (rare — RFC 3501 mandates
it on all standard servers), `sinceUIDInitialized` stays false and
`fetchNewMessages` refuses to issue any FETCH at all. The session sits in
IDLE waiting for fresh arrivals; no backlog is ever pulled. Mail visibility
is sacrificed in this edge case to preserve the "bounded memory" invariant.

## Reconnect model

| Error type | Detected via | Response |
|---|---|---|
| Transport (closed pipe, write error, read EOF) | underlying conn error during exec/idle | exponential backoff reconnect; sinceUID preserved; force-FETCH on resume to catch mail from outage |
| Protocol-level IDLE rejection (BAD/NO continuation, non-OK terminator) | `errIDLERejected` sentinel | exit watch loop cleanly; do NOT reconnect (a fresh conn hits the same rejection) |
| Parent ctx cancel during reconnect backoff | `ctx.Done()` in `time.NewTimer` select | abandon retries, close watch loop, let teardown finish |

Backoff doubles: 1s → 2s → 4s → … capped at 60s. Reconnect attempts are
cancellable; `Manager.Shutdown` interrupts a mid-backoff retry within
milliseconds.

## Concurrency model

| Resource | Protection |
|---|---|
| `Manager.sessions` map | `Manager.mu` Mutex |
| `session.refs` | `atomic.Int32`; lifecycle transitions checked under `Manager.mu` |
| `session.state` | `atomic.Int32` (New / Active / Closed) |
| `session.conn`, `session.selectedFolder` | `session.connMu` Mutex |
| `session.subs`, `session.subsByTo` | `session.subsMu` Mutex |
| `session.idleRoundCancel` | `session.idleMu` Mutex |
| `session.sinceUID`, `session.sinceUIDInitialized` | `atomic.Uint32` / `atomic.Bool` |
| `session.pendingFetch` | `atomic.Bool` |
| `imapConn.execMu` | serializes ALL commands (incl. IDLE for its full duration) |
| `imapConn.tagCtr` | unprotected, only accessed via exec while holding `execMu` |

The IDLE write of `DONE` from the watcher goroutine runs concurrently with
the reader on the same `net.Conn`. This is explicitly safe per Go's
`net.Conn` contract: "Multiple goroutines may invoke methods on a Conn
simultaneously."

## Dispatch and filter index

When a FETCH response comes back, each message is dispatched via:

```go
func (s *session) dispatch(msg Message) {
    tk := toKey(msg.To)                  // lowercased

    s.subsMu.Lock()
    defer s.subsMu.Unlock()

    candidates := s.subsByTo[tk]         // exact-To matches
    if tk != "" {
        candidates = append(candidates,
            s.subsByTo[""]...)           // plus wildcard subs
    }
    for _, sub := range candidates {
        if !matchFilter(sub.filter, msg, now) { continue }
        deliverOrDropOldest(sub.ch, msg)
    }
}
```

- `subsByTo` is keyed by the lowercased `Filter.To` (or `""` for wildcard subs).
  Two O(1) lookups per dispatch instead of a linear scan over thousands of
  subscriptions.
- All sends are non-blocking: `select { case ch <- msg: default: }`. On a
  full per-subscriber buffer (capacity 8), the oldest message is dropped to
  make room for the newest. A hung consumer cannot stall the dispatcher or
  pin memory.
- Iteration runs under `subsMu` so a concurrent `Close` cannot land between
  the read of `subs` and the channel send.

## Memory footprint

| Component | Memory cost |
|---|---|
| Per-session | ~1 KB plus an 8 KB bufio.Reader |
| Per-subscription | ~200 bytes + 8 × (Message struct + body string ref) |
| Per FETCH'd message body | held only inside subscribers' channels; freed by GC once consumed (or dropped on overflow) |

A million-message backlog never enters the process. The Manager allocates
on the order of *(unique credentials × 9 KB) + (subscriptions × 200 B)*.

## Things we deliberately did not build

- **Multiple folders per session.** SELECT INBOX is the only mailbox. Spam,
  custom labels, etc. are out of scope; add them when there's a clear need.
- **Message flag manipulation.** No mark-as-read, no flag updates. We use
  `BODY.PEEK[...]` so reads don't side-effect the mailbox.
- **Custom search criteria beyond Filter.** Most subscribers want "mail to
  me from anywhere." Filter handles the common case in 5 fields.
- **MIME decoding.** Bodies are returned raw. If you need decoded HTML or
  attachments, wrap with `net/mail` or `enmime` at the call site.
- **OAuth2 (XOAUTH2).** Most catch-alls run on hosted servers using app
  passwords. If/when a real need arises, the LOGIN path generalizes cleanly.
- **STARTTLS.** TLS-from-the-start (port 993) only. Modern providers don't
  support cleartext-to-STARTTLS upgrades anyway.
