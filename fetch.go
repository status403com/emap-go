package emap

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// rawFetchResp is the unparsed pre-shape of a single `* N FETCH (...)`
// response. Headers and body sit in separate byte slices because the FETCH
// protocol returns them as literal blocks; the rest of the atoms are pulled
// out of the inlined line via regex.
type rawFetchResp struct {
	seq          int
	uid          uint32
	internalDate time.Time
	headers      []byte
	body         []byte
}

var (
	fetchSeqRE          = regexp.MustCompile(`^\*\s+(\d+)\s+FETCH\s*\(`)
	fetchUIDRE          = regexp.MustCompile(`\bUID\s+(\d+)`)
	fetchInternalDateRE = regexp.MustCompile(`\bINTERNALDATE\s+"([^"]+)"`)
	// Matches BODY[<section>] followed by our literal placeholder. The
	// section can contain parens (e.g. HEADER.FIELDS (TO FROM SUBJECT DATE))
	// but never `]`, so ([^\]]+) is safe.
	fetchBodyLitRE = regexp.MustCompile(`BODY\[([^\]]+)\]\s*\x00L(\d+)\x00`)
)

// internalDateLayout matches IMAP's INTERNALDATE shape (RFC 3501 §9):
// `dd-Mon-yyyy HH:MM:SS ±zzzz`. Day is single- or double-digit; Go's `2` and
// `02` parse both.
const internalDateLayout = "2-Jan-2006 15:04:05 -0700"

// parseFetchResponse extracts the structured fields from one `* N FETCH (...)`
// line. literals are the captured literal blocks from readFetchLine. Returns
// (zero, false) if the line isn't a recognizable FETCH response — caller
// silently skips those (CAPABILITY-style untagged lines, server hints, etc.).
func parseFetchResponse(line string, literals [][]byte) (rawFetchResp, bool) {
	r := rawFetchResp{}

	m := fetchSeqRE.FindStringSubmatch(line)
	if m == nil {
		return r, false
	}
	if n, err := strconv.Atoi(m[1]); err == nil {
		r.seq = n
	}

	if m := fetchUIDRE.FindStringSubmatch(line); m != nil {
		if n, err := strconv.ParseUint(m[1], 10, 32); err == nil {
			r.uid = uint32(n)
		}
	}

	if m := fetchInternalDateRE.FindStringSubmatch(line); m != nil {
		if t, err := time.Parse(internalDateLayout, m[1]); err == nil {
			r.internalDate = t
		}
	}

	for _, m := range fetchBodyLitRE.FindAllStringSubmatch(line, -1) {
		section := m[1]
		idx, _ := strconv.Atoi(m[2])
		if idx < 0 || idx >= len(literals) {
			continue
		}
		switch {
		case strings.Contains(strings.ToUpper(section), "HEADER"):
			r.headers = literals[idx]
		case strings.EqualFold(section, "TEXT"):
			r.body = literals[idx]
		}
	}

	return r, true
}

// parseHeaderFields extracts To/From/Subject/Date from an IMAP
// BODY[HEADER.FIELDS] literal. Uses net/mail for RFC 5322 conformance —
// address-list parsing handles `"Display" <bare@addr>` forms transparently.
//
// IMAP returns headers terminated with an empty CRLF line. We feed the buffer
// to mail.ReadMessage with a synthesized empty body delimiter (already
// present in IMAP responses) so net/mail's reader is satisfied.
func parseHeaderFields(raw []byte) (to, from, subject string, date time.Time) {
	if len(raw) == 0 {
		return
	}
	// net/mail.ReadMessage requires headers + at least an empty body. IMAP
	// already includes the terminating CRLFCRLF; if not, append it.
	buf := raw
	if !bytes.HasSuffix(raw, []byte("\r\n\r\n")) {
		buf = append(buf, '\r', '\n', '\r', '\n')
	}
	msg, err := mail.ReadMessage(bytes.NewReader(buf))
	if err != nil {
		return
	}
	to = extractBareAddr(msg.Header.Get("To"))
	from = extractBareAddr(msg.Header.Get("From"))
	subject = msg.Header.Get("Subject")
	if d, err := mail.ParseDate(msg.Header.Get("Date")); err == nil {
		date = d
	}
	return
}

// extractBareAddr strips the display-name portion of an RFC 5322 address,
// returning just `local@host`. Falls back to the raw value on parse failure
// so we don't silently drop weird-but-valid addresses.
func extractBareAddr(headerValue string) string {
	if headerValue == "" {
		return ""
	}
	if a, err := mail.ParseAddress(headerValue); err == nil {
		return a.Address
	}
	return headerValue
}

// fetchNewMessages runs UID FETCH for everything strictly above sinceUID,
// parses each response into a Message, and dispatches to subscribers.
// Updates sinceUID atomically to the highest UID seen so we don't re-fetch
// the same mail on the next IDLE round.
//
// Designed to be cheap for inboxes of any size: the UID range starts at
// sinceUID+1, which on first connect is set to UIDNEXT (skipping the
// entire backlog). Subsequent calls only pull mail that arrived since the
// previous round.
//
// Refuses to FETCH when sinceUID isn't initialized — that guards against
// non-conformant servers omitting UIDNEXT, where a naive `FETCH 1:*` would
// pull the entire inbox.
func (s *session) fetchNewMessages(c *imapConn) error {
	if !s.sinceUIDInitialized.Load() {
		return nil
	}
	since := s.sinceUID.Load()
	// `<N>:*` matches every UID >= N. The server returns nothing if no such
	// messages exist — empty response is the common-case for an IDLE round
	// where the EXISTS happened to be for mail we've already fetched.
	cmd := fmt.Sprintf(
		"UID FETCH %d:* (UID INTERNALDATE BODY.PEEK[HEADER.FIELDS (TO FROM SUBJECT DATE)] BODY.PEEK[TEXT])",
		since+1,
	)
	resps, err := c.execFetch(cmd)
	if err != nil {
		return err
	}

	maxUID := since
	for _, r := range resps {
		if r.uid <= since {
			continue
		}
		if r.uid > maxUID {
			maxUID = r.uid
		}
		msg := buildMessageFromFetch(r)
		s.dispatch(msg)
	}
	// Atomically bump the watermark. If a concurrent FETCH set a higher
	// value (shouldn't happen — fetches are serialized by execMu — but
	// defend anyway), keep the higher.
	for {
		cur := s.sinceUID.Load()
		if maxUID <= cur {
			break
		}
		if s.sinceUID.CompareAndSwap(cur, maxUID) {
			break
		}
	}
	return nil
}

// backfillMaxMessages caps a Filter.Backfill historical fetch to the newest
// N matching UIDs so an opt-in backfill can never pull a whole inbox.
const backfillMaxMessages = 500

// imapDateLayout is the RFC 3501 `date` shape used by SEARCH SINCE.
const imapDateLayout = "02-Jan-2006"

// runBackfill performs sub's one-time historical fetch: UID SEARCH SINCE
// the day before the filter's Since (narrowed by TO/FROM when set), FETCH the
// newest backfillMaxMessages hits at or below sub.backfillMax, and deliver
// matches to sub only. SINCE compares day-granular dates in the server's
// timezone, which may lag Since's zone by up to a day — hence the extra day;
// matchFilter applies the precise Since gate. UIDs above backfillMax were
// dispatched after sub registered, so the live path already delivers them.
// Never touches sinceUID.
//
// Returns only transport errors (the conn may be mid-response and must be
// replaced); a server rejection drops this backfill and returns nil.
func (s *session) runBackfill(c *imapConn, sub *Subscription) error {
	if !s.isSubscribed(sub) {
		return nil
	}
	f := sub.filter
	criteria := "SINCE " + f.Since.AddDate(0, 0, -1).Format(imapDateLayout)
	if f.To != "" {
		criteria += ` TO "` + escapeQ(f.To) + `"`
	}
	if f.FromContains != "" {
		// IMAP FROM is a case-insensitive substring match on the From header,
		// a superset of FromContains; matchFilter re-checks below.
		criteria += ` FROM "` + escapeQ(f.FromContains) + `"`
	}
	uids, err := c.uidSearch(criteria)
	if err != nil {
		return transportErr(err)
	}
	uids = slices.DeleteFunc(uids, func(u uint32) bool { return u > sub.backfillMax })
	if len(uids) == 0 {
		return nil
	}
	slices.SortFunc(uids, func(a, b uint32) int { return cmp.Compare(b, a) })
	if len(uids) > backfillMaxMessages {
		uids = uids[:backfillMaxMessages]
	}
	set := make([]string, len(uids))
	for i, u := range uids {
		set[i] = strconv.FormatUint(uint64(u), 10)
	}
	resps, err := c.execFetch("UID FETCH " + strings.Join(set, ",") +
		" (UID INTERNALDATE BODY.PEEK[HEADER.FIELDS (TO FROM SUBJECT DATE)] BODY.PEEK[TEXT])")
	if err != nil {
		return transportErr(err)
	}

	now := time.Now()
	tk := toKey(f.To)
	var msgs []Message
	for _, r := range resps {
		msg := buildMessageFromFetch(r)
		if tk != "" && toKey(msg.To) != tk {
			continue
		}
		if matchFilter(f, msg, now) {
			msgs = append(msgs, msg)
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	if _, live := s.subs[sub]; !live {
		return nil
	}
	for _, msg := range msgs {
		deliverOrDropOldest(sub.ch, msg)
	}
	return nil
}

// transportErr passes err through unless it is a server rejection.
func transportErr(err error) error {
	if errors.Is(err, errCommandRejected) {
		return nil
	}
	return err
}

// buildMessageFromFetch converts a parsed FETCH response into the
// subscriber-facing Message.
//
// Message.Date is set from INTERNALDATE (server's view of arrival) rather
// than the header Date — the header Date is sender-controlled and can lie or
// drift seconds away from subscribe time, which races with Filter.Since.
// INTERNALDATE comes from the same clock the IMAP server uses to time-stamp
// arrivals, so "subscribed at T, want mail arrived after T" is well-defined.
func buildMessageFromFetch(r rawFetchResp) Message {
	to, from, subject, _ := parseHeaderFields(r.headers)
	return Message{
		UID:     r.uid,
		To:      to,
		From:    from,
		Subject: subject,
		Date:    r.internalDate,
		Body:    string(r.body),
	}
}
