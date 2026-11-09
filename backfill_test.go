package emap

import (
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// backfillServer scripts a fake IMAP server for backfill tests: bring-up
// with UIDNEXT 100 (IDLE advertised or not), UID SEARCH answered with the
// UIDs of history, UID FETCH <set> with the requested history messages, live
// UID FETCH <n>:* with live messages whose UID >= n, and IDLE heartbeats.
type backfillServer struct {
	idle    bool
	history []fakeMail
	live    []fakeMail
	// onSearch, if set, picks how the n-th SEARCH (1-based, across all
	// connections) is answered: "" normal, "drop" closes the connection,
	// "NO" sends a tagged rejection.
	onSearch func(n int32) string

	searches   atomic.Int32
	lastSearch atomic.Value
}

func (b *backfillServer) script(fc *fakeConn) {
	if b.idle {
		handleIdleBringUpWithUIDNext(fc, 100)
	} else {
		handleNoIdleBringUp(fc, 100)
	}
	for {
		tag, rest, err := fc.readCommand()
		if err != nil {
			return
		}
		up := strings.ToUpper(rest)
		switch {
		case up == "IDLE":
			_ = fc.write("+ idling\r\n")
			_ = fc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			line, err := fc.r.ReadString('\n')
			_ = fc.conn.SetReadDeadline(time.Time{})
			if err != nil || !strings.EqualFold(strings.TrimSpace(line), "DONE") {
				return
			}
			_ = fc.write(tag + " OK IDLE completed\r\n")
		case strings.HasPrefix(up, "UID SEARCH "):
			n := b.searches.Add(1)
			b.lastSearch.Store(rest[len("UID SEARCH "):])
			mode := ""
			if b.onSearch != nil {
				mode = b.onSearch(n)
			}
			switch mode {
			case "drop":
				return
			case "NO":
				_ = fc.write(tag + " NO search not allowed\r\n")
				continue
			}
			uids := make([]string, len(b.history))
			for i, m := range b.history {
				uids[i] = itoa(m.UID)
			}
			_ = fc.write("* SEARCH " + strings.Join(uids, " ") + "\r\n")
			_ = fc.write(tag + " OK SEARCH completed\r\n")
		case strings.HasPrefix(up, "UID FETCH "):
			set := strings.Fields(rest)[2]
			seq := 1
			if start, ok := strings.CutSuffix(set, ":*"); ok {
				from, _ := strconv.ParseUint(start, 10, 32)
				for _, m := range b.live {
					if uint64(m.UID) >= from {
						writeFetchResponse(fc, seq, m)
						seq++
					}
				}
			} else {
				requested := map[string]bool{}
				for _, u := range strings.Split(set, ",") {
					requested[u] = true
				}
				for _, m := range b.history {
					if requested[itoa(m.UID)] {
						writeFetchResponse(fc, seq, m)
						seq++
					}
				}
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

func itoa(u uint32) string { return strconv.FormatUint(uint64(u), 10) }

func backfillHistory(now time.Time) []fakeMail {
	at := func(d time.Duration) string { return now.Add(d).UTC().Format(internalDateLayout) }
	return []fakeMail{
		// Matches To, after Since: must be delivered.
		{UID: 42, To: "alice@example.com", From: "noreply@vendor.com", Subject: "Key", InternalDate: at(-2 * time.Hour), Body: "key cb_1"},
		// Older than Since (same day at SEARCH granularity): dropped.
		{UID: 41, To: "alice@example.com", From: "noreply@vendor.com", Subject: "Old", InternalDate: at(-4 * time.Hour), Body: "old"},
		// Different recipient (server TO is substring-only): dropped.
		{UID: 43, To: "malice@example.com", From: "noreply@vendor.com", Subject: "Other", InternalDate: at(-1 * time.Hour), Body: "other"},
	}
}

func newBackfillManager(t *testing.T, b *backfillServer) (*Manager, *fakeServer) {
	t.Helper()
	srv := newFakeServer()
	srv.script = b.script
	m := NewManager(time.Hour).withDial(srv.dialer())
	m.PollInterval = 25 * time.Millisecond
	t.Cleanup(m.Shutdown)
	return m, srv
}

func backfillFilter(now time.Time) Filter {
	return Filter{To: "alice@example.com", Since: now.Add(-3 * time.Hour), Backfill: true}
}

// collect drains sub.Ch for d and returns the delivered UIDs.
func collect(sub *Subscription, d time.Duration) []uint32 {
	var got []uint32
	timeout := time.After(d)
	for {
		select {
		case msg, ok := <-sub.Ch:
			if !ok {
				return got
			}
			got = append(got, msg.UID)
		case <-timeout:
			return got
		}
	}
}

func TestBackfillDeliversHistoricalMail(t *testing.T) {
	for _, mode := range []struct {
		name string
		idle bool
	}{{"idle", true}, {"poll", false}} {
		t.Run(mode.name, func(t *testing.T) {
			now := time.Now()
			b := &backfillServer{idle: mode.idle, history: backfillHistory(now)}
			m, _ := newBackfillManager(t, b)

			// A plain subscriber first, so the backfill subscriber joins a
			// session whose loop is already running (IDLE wake path).
			plain, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{To: "alice@example.com"})
			if err != nil {
				t.Fatal(err)
			}
			defer plain.Close()
			time.Sleep(50 * time.Millisecond)

			sub, err := m.Subscribe(sampleCred("inbox@example.com"), Filter{
				To:           "alice@example.com",
				FromContains: "vendor",
				Since:        now.Add(-3 * time.Hour),
				Backfill:     true,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()

			if got := collect(sub, 400*time.Millisecond); len(got) != 1 || got[0] != 42 {
				t.Fatalf("delivered UIDs %v, want [42]", got)
			}
			select {
			case msg := <-plain.Ch:
				t.Fatalf("backfill leaked UID %d to a non-backfill subscriber", msg.UID)
			default:
			}
			if got := b.searches.Load(); got != 1 {
				t.Fatalf("UID SEARCH issued %d times, want 1", got)
			}
			// One day before Since, so a server whose timezone lags Since's
			// still includes mail from Since's calendar day.
			want := `SINCE ` + now.Add(-3*time.Hour).AddDate(0, 0, -1).Format("02-Jan-2006") +
				` TO "alice@example.com" FROM "vendor"`
			if got := b.lastSearch.Load(); got != want {
				t.Fatalf("SEARCH criteria = %q, want %q", got, want)
			}
		})
	}
}

// Mail that arrives after subscribe is visible to both SEARCH and the live
// FETCH; the subscriber must still get it exactly once.
func TestBackfillDoesNotDuplicateLiveMail(t *testing.T) {
	now := time.Now()
	fresh := fakeMail{UID: 150, To: "alice@example.com", From: "noreply@vendor.com", Subject: "Key", Body: "key"}
	b := &backfillServer{history: []fakeMail{fresh}, live: []fakeMail{fresh}}
	m, _ := newBackfillManager(t, b)

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), backfillFilter(now))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if got := collect(sub, 400*time.Millisecond); len(got) != 1 || got[0] != 150 {
		t.Fatalf("delivered UIDs %v, want exactly [150]", got)
	}
}

// A dropped connection mid-backfill must not lose the backfill: the loop
// reconnects and retries it.
func TestBackfillRetriesAfterTransportError(t *testing.T) {
	prev := ReconnectInitialBackoff
	ReconnectInitialBackoff = 10 * time.Millisecond
	defer func() { ReconnectInitialBackoff = prev }()

	now := time.Now()
	b := &backfillServer{
		history: backfillHistory(now),
		onSearch: func(n int32) string {
			if n == 1 {
				return "drop"
			}
			return ""
		},
	}
	m, srv := newBackfillManager(t, b)

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), backfillFilter(now))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if got := collect(sub, time.Second); len(got) != 1 || got[0] != 42 {
		t.Fatalf("delivered UIDs %v after reconnect, want [42]", got)
	}
	if got := srv.dialsCt.Load(); got < 2 {
		t.Fatalf("dials = %d, want a reconnect", got)
	}
}

// A server rejection is final: no retry loop, no reconnect, no delivery.
func TestBackfillRejectedSearchIsDropped(t *testing.T) {
	now := time.Now()
	b := &backfillServer{
		history:  backfillHistory(now),
		onSearch: func(int32) string { return "NO" },
	}
	m, srv := newBackfillManager(t, b)

	sub, err := m.Subscribe(sampleCred("inbox@example.com"), backfillFilter(now))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if got := collect(sub, 300*time.Millisecond); len(got) != 0 {
		t.Fatalf("rejected backfill delivered %v", got)
	}
	if got := b.searches.Load(); got != 1 {
		t.Fatalf("UID SEARCH issued %d times, want 1 (no retry)", got)
	}
	if got := srv.dialsCt.Load(); got != 1 {
		t.Fatalf("dials = %d, want 1 (no reconnect)", got)
	}
}

func TestBackfillDisabledByDefault(t *testing.T) {
	now := time.Now()
	b := &backfillServer{history: backfillHistory(now)}
	m, _ := newBackfillManager(t, b)

	f := backfillFilter(now)
	f.Backfill = false
	sub, err := m.Subscribe(sampleCred("inbox@example.com"), f)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if got := collect(sub, 200*time.Millisecond); len(got) != 0 {
		t.Fatalf("Backfill=false delivered historical UIDs %v", got)
	}
	if got := b.searches.Load(); got != 0 {
		t.Fatalf("UID SEARCH issued %d times with Backfill=false", got)
	}
}
