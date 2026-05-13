# emap-go — Efficient IMAP

A high-fanout IMAP client for Go, built for workloads where many independent
tasks need to read mail from a small set of inboxes.

```
import "github.com/status403com/emap-go"
```

## The problem this solves

You have N tasks (automation, account verification, bots, scrapers — whatever)
that each need to receive mail at a unique address served by a shared catch-all
mailbox. With a typical IMAP client:

- Every task opens its own IMAP connection → you hit the per-account connection
  cap fast (Gmail allows 15 simultaneous connections per account; the rest get
  rejected).
- Each connection downloads the full inbox on startup, or at least the
  metadata for it — an inbox with a million messages takes minutes and
  hundreds of MB before you can do anything useful.
- Connections drop and don't reconnect, or reconnect blindly without backoff
  and get the account rate-limited.
- The Go IMAP libraries that handle most of this correctly bring 30+
  transitive dependencies, have known memory leaks under sustained load, and
  use one-Dialer-per-user mental models that fight you when many subscribers
  want the same mailbox.

**emap-go is designed around the fanout case from the ground up.** One
persistent IMAP connection per credential, shared by every subscriber to that
mailbox. No backlog fetch, ever. IDLE-first push delivery with polling
fallback. No third-party dependencies beyond the Go stdlib.

## Key advantages

| | emap-go | typical Go IMAP libs |
|---|---|---|
| **2000 tasks on 5 inboxes** | 5 TCP connections, pooled | 2000 connections, dies on Gmail's 15-conn cap |
| **Inbox with 1M backlog** | 0 ms cost at startup (UIDNEXT skip) | seconds to minutes of indexing |
| **Memory under load** | bounded — per-subscriber cap 8 messages with drop-oldest | grows with inbox size + subscriber count |
| **Dependencies** | stdlib + `net/mail` only | typically 20–40 transitive deps |
| **Reconnect** | exponential backoff, transport-error-only | varies; some libs hammer servers on protocol rejection |
| **Mail freshness** | IDLE push (≤1s); poll fallback | usually polling at fixed cadence |
| **Concurrent subscribers per mailbox** | unlimited, share one conn | each subscriber typically opens its own conn |

## Quick start

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/status403com/emap-go"
)

func main() {
    mgr := emap.NewManager(emap.DefaultLinger)
    defer mgr.Shutdown()

    cred := emap.Credential{
        Host:     "imap.gmail.com",
        Port:     993,
        UseTLS:   true,
        Email:    "you@example.com",
        Password: "your-app-password",
    }

    sub, err := mgr.Subscribe(cred, emap.Filter{
        To:           "verify+task123@example.com",
        FromContains: "no-reply@vendor",
        MaxAge:       5 * time.Minute,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Close()

    select {
    case msg := <-sub.Ch:
        fmt.Printf("verification mail: %s\n", msg.Body)
    case <-time.After(60 * time.Second):
        log.Println("timeout — no mail arrived")
    }
}
```

## Subscribing many tasks to one mailbox

This is the case emap-go is built for. **Every Subscribe to the same
credential reuses the same underlying TCP connection.**

```go
mgr := emap.NewManager(emap.DefaultLinger)
defer mgr.Shutdown()

cred := emap.Credential{Host: "imap.example.com", Port: 993, UseTLS: true,
                        Email: "catchall@example.com", Password: "pw"}

// 2000 tasks subscribe to per-task addresses on a catch-all.
// Result: 1 IMAP connection, 1 IDLE loop, 2000 subscriptions fan-out.
for i := 0; i < 2000; i++ {
    addr := fmt.Sprintf("task%d@example.com", i)
    sub, err := mgr.Subscribe(cred, emap.Filter{To: addr})
    if err != nil { log.Fatal(err) }
    go handleVerification(sub) // each task reads its own channel
}
```

## Bound to a context

For task-style workloads where each subscription has a parent context,
the `Mailbox` adapter wires `ctx.Done()` to `Subscription.Close()` so you
never leak a sub on cancellation:

```go
mb := emap.NewMailbox(mgr, cred)

ch, err := mb.Watch(ctx, emap.Filter{To: taskAddr})
if err != nil { return err }
// No defer needed — when ctx is canceled, the subscription auto-closes.

select {
case msg := <-ch:
    // ...
case <-ctx.Done():
    return ctx.Err()
}
```

## Configuration

All knobs are package-level vars and can be tuned before constructing a
Manager. Sensible defaults for production:

```go
emap.DefaultLinger              // 60s — how long a session stays warm after the last unsubscribe
emap.IdleRoundDuration          // 25 min — IDLE re-issue cadence (RFC 2177 limit is 29 min)
emap.PollInterval               // 30s — fallback poll cadence when server lacks IDLE
emap.ReconnectInitialBackoff    //  1s — first reconnect retry delay
emap.ReconnectMaxBackoff        // 60s — reconnect retry cap
emap.ConnDialTimeout            // 10s — TCP/TLS handshake limit
emap.ConnGreetingTimeout        // 10s — server greeting read limit
emap.ConnCommandTimeout         // 30s — per-command read limit (does not apply to IDLE)
```

## API surface

| Type | Purpose |
|---|---|
| `Credential` | Mailbox identity: host/port/TLS/email/password. |
| `Manager` | Connection pool. One per process. |
| `Mailbox` | Credential-bound subscription factory; auto-closes on ctx cancel. |
| `Subscription` | Live mail stream + Close. |
| `Filter` | To / FromContains / Since / MaxAge selectors. |
| `Message` | UID / To / From / Subject / Date / Body. |

| Function | Purpose |
|---|---|
| `NewManager(linger time.Duration) *Manager` | Construct the pool. |
| `(*Manager).Subscribe(cred, filter) (*Subscription, error)` | Open a new subscription. |
| `(*Manager).Shutdown()` | Close everything cleanly. |
| `NewMailbox(mgr, cred) Mailbox` | Wrap a credential for `Watch(ctx, filter)`. |
| `TestLogin(host, port, useTLS, email, password) error` | One-shot credential check. |

## How it skips backlog

After the SELECT command, the server returns `* OK [UIDNEXT N]` — the next
UID it will assign to a new message. emap-go captures N and treats every
UID < N as already-seen. The first IMAP FETCH command emap-go ever issues is
`UID FETCH N:*` — strictly above the watermark. **On first connect this
returns zero messages**, no matter how large the inbox.

When you reconnect, the watermark survives, so mail that arrived during the
disconnect window is still picked up on resume. If the server doesn't return
UIDNEXT (rare; non-conformant), emap-go **refuses to FETCH** rather than
fall back to `FETCH 1:*` and pull the entire inbox. Better to deliver
nothing than to dump 1M messages into memory.

See [ARCHITECTURE.md](ARCHITECTURE.md) for full design details.

## Status

- 59 tests under `-race`, stable across repeated runs.
- No known leaks under 2000-subscription stress.
- Production-grade in terms of correctness; not yet battle-tested at scale
  in the wild — please report issues.

## License

MIT (see LICENSE).
