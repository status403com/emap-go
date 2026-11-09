# emap-go — Efficient IMAP

A high-fanout IMAP client for Go, built for workloads where many independent
tasks need to read mail from a small set of inboxes.

📖 [Technical write-up](https://status403.com/blog/emap-go-high-fanout-imap?utm_source=github&utm_medium=referral&utm_campaign=emap-technical)

```
import "github.com/status403com/emap-go/v2"
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
mailbox. No backlog fetch unless a subscriber explicitly opts in
(`Filter.Backfill`). IDLE-first push delivery with polling
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

    "github.com/status403com/emap-go/v2"
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

## Microsoft Outlook / Hotmail / Microsoft 365

Microsoft mailboxes reject password `LOGIN` (`NO Basic authentication is
disabled`) and require OAuth2, which emap-go speaks via SASL `XOAUTH2` when
`Credential.OAuth2` is set.

Prerequisite: a Microsoft Entra app registration with

- supported account types that include the mailbox (personal Microsoft
  accounts for `@hotmail.com` / `@outlook.com` / `@live.com`),
- **Allow public client flows** enabled (Authentication → Advanced settings),
- the delegated API permission *Office 365 Exchange Online →
  IMAP.AccessAsUser.All*.

Sign the mailbox owner in once with the device-code flow and persist the
refresh token:

```go
oauth := emap.MicrosoftOAuth{ClientID: "<application-client-id>"}

dc, err := oauth.StartDeviceLogin(ctx)
if err != nil { return err }
// Hand the code to your UI. dc.Message is Microsoft's ready-made text:
// "To sign in, open https://microsoft.com/devicelogin and enter …"
show(dc.UserCode, dc.VerificationURI, dc.ExpiresAt)

refreshToken, err := dc.Wait(ctx) // blocks until the owner signs in
if err != nil { return err }
saveRefreshToken(refreshToken)
```

Then connect with the token source instead of a password:

```go
cred := emap.Credential{
    Host:   "outlook.office365.com",
    Port:   993,
    UseTLS: true,
    Email:  "someone@hotmail.com",
    OAuth2: oauth.TokenSource(loadRefreshToken(), saveRefreshToken),
}
```

The token source caches each access token until shortly before expiry and
refreshes on demand (initial connect and every reconnect). Microsoft rotates
refresh tokens on use; the `onRotate` callback (`saveRefreshToken` above)
receives each new one so the next process start doesn't present a stale
token. Any other provider works too: implement `emap.TokenSource`.

When the refresh token is expired or revoked, or Microsoft demands new
interaction (MFA, consent), the error is an `*emap.ReauthRequiredError`
naming the mailbox (`Host`, `Port`, `Email`); it also matches
`errors.Is(err, emap.ErrReauthRequired)`. The library never retries it:

- `Subscribe` and `TestLogin` return it directly.
- A live session that hits it on reconnect is evicted: every subscription's
  `Ch` closes and `sub.Err()` returns the error.

```go
var reauth *emap.ReauthRequiredError
if errors.As(err, &reauth) {          // err from Subscribe, or sub.Err() after Ch closed
    promptSignIn(reauth.Email)        // StartDeviceLogin → Wait → save the new refresh token
}
```

After the new sign-in, `Subscribe` with the new token source builds a fresh
session. `Mailbox.Watch` callers only see the channel close; their next
`Watch` returns the error.

## Polling vs IDLE

By default emap-go uses IMAP IDLE for push-style delivery when the server
supports it, falling back to polling when it doesn't.

**Gmail and IDLE latency:** Gmail's IMAP server delays IDLE EXISTS
notifications by 20–60 seconds, even though the same mail appears instantly
in the Gmail web UI and mobile apps (which use a proprietary push channel,
not IMAP IDLE). If you need lower latency on Gmail, force polling mode —
at a 3-second interval the worst-case delay drops from ~60s to ~3s:

```go
mgr := emap.NewManager(emap.DefaultLinger)
mgr.ForcePolling = true              // skip IDLE, poll instead
mgr.PollInterval = 5 * time.Second   // default is 3s
```

Polling is per-credential, not per-subscriber — 2000 tasks on one inbox
still means one FETCH every interval, not 2000.

## Configuration

All knobs are package-level vars and can be tuned before constructing a
Manager. Sensible defaults for production:

```go
emap.DefaultLinger              // 60s — how long a session stays warm after the last unsubscribe
emap.IdleRoundDuration          // 25 min — IDLE re-issue cadence (RFC 2177 limit is 29 min)
mgr.PollInterval                //  3s — poll cadence (used when IDLE unavailable or ForcePolling set)
emap.ReconnectInitialBackoff    //  1s — first reconnect retry delay
emap.ReconnectMaxBackoff        // 60s — reconnect retry cap
emap.ConnDialTimeout            // 10s — TCP/TLS handshake limit
emap.ConnGreetingTimeout        // 10s — server greeting read limit
emap.ConnCommandTimeout         // 30s — per-command read limit (does not apply to IDLE)
```

## API surface

| Type | Purpose |
|---|---|
| `Credential` | Mailbox identity: host/port/TLS/email plus password or OAuth2 token source. |
| `TokenSource` | Supplies OAuth2 access tokens for XOAUTH2 (`Token(ctx)`). |
| `ReauthRequiredError` | Names the mailbox that needs a new sign-in; matches `ErrReauthRequired`. |
| `MicrosoftOAuth` | Device-code sign-in and refreshing token source for Outlook / Microsoft 365. |
| `Manager` | Connection pool. One per process. |
| `Mailbox` | Credential-bound subscription factory; auto-closes on ctx cancel. |
| `Subscription` | Live mail stream + Close; `Err()` says why the library closed `Ch`. |
| `Filter` | To / FromContains / Since / MaxAge selectors; opt-in `Backfill` for pre-existing mail since `Since`. |
| `Message` | UID / To / From / Subject / Date / Body. |

| Function | Purpose |
|---|---|
| `NewManager(linger time.Duration) *Manager` | Construct the pool. |
| `(*Manager).Subscribe(cred, filter) (*Subscription, error)` | Open a new subscription. |
| `(*Manager).Shutdown()` | Close everything cleanly. |
| `NewMailbox(mgr, cred) Mailbox` | Wrap a credential for `Watch(ctx, filter)`. |
| `TestLogin(cred) error` | One-shot credential check (LOGIN or XOAUTH2). |
| `(MicrosoftOAuth).StartDeviceLogin(ctx) (*DeviceCode, error)` | Begin device-code sign-in; returns the code to display. |
| `(*DeviceCode).Wait(ctx) (refreshToken, error)` | Block until the owner approves (or declines / code expires). |
| `(MicrosoftOAuth).TokenSource(refreshToken, onRotate) TokenSource` | Caching, refreshing XOAUTH2 token source. |

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

**Opt-in backfill.** A subscriber that sets `Filter.Backfill: true` with a
non-zero `Since` additionally gets a one-time `UID SEARCH SINCE <Since − 1 day>`
(narrowed by `TO`/`FROM` when `Filter.To`/`FromContains` are set) plus a FETCH of the newest 500
hits, delivered to that subscriber only. The shared watermark is untouched,
so other subscribers never see backlog. Useful when the mail you wait for
may have landed before you subscribed (e.g. a key email hours later).

See [ARCHITECTURE.md](ARCHITECTURE.md) for full design details.

## Status

- 72 tests under `-race`, stable across repeated runs.
- No known leaks under 2000-subscription stress.
- Production-grade in terms of correctness; not yet battle-tested at scale
  in the wild — please report issues.

## License

MIT (see LICENSE).
