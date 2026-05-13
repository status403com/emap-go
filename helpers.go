package emap

import (
	"fmt"
	"strconv"
	"strings"
)

// parseLiteralSuffix returns N and true when line ends with `{N}` or `{N+}`
// — the IMAP literal length marker (RFC 3501 §4.3, with `{N+}` from RFC 7888
// non-synchronizing literals). Otherwise returns (0, false).
//
// Used by readResponseLine and readFetchLine to detect when the next read
// must consume exactly N raw bytes of literal data before resuming
// line-oriented parsing.
func parseLiteralSuffix(line string) (int, bool) {
	if !strings.HasSuffix(line, "}") {
		return 0, false
	}
	i := strings.LastIndexByte(line, '{')
	if i < 0 {
		return 0, false
	}
	inner := line[i+1 : len(line)-1]
	// LITERAL+ (RFC 7888) appends `+` inside the braces: `{N+}`.
	inner = strings.TrimSuffix(inner, "+")
	n, err := strconv.Atoi(inner)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// expectOK scans the lines collected by exec() for the tagged response and
// returns nil when it carries `OK`, or an error otherwise. Used after every
// IMAP command that returns a single tagged terminator (LOGIN, SELECT,
// CAPABILITY, etc.).
func expectOK(tag string, lines []string, op string) error {
	for _, l := range lines {
		if !strings.HasPrefix(l, tag+" ") {
			continue
		}
		if strings.Contains(l, " OK") {
			return nil
		}
		return fmt.Errorf("%s failed: %s", op, strings.TrimSpace(l))
	}
	return fmt.Errorf("%s: no tagged response", op)
}

// escapeQ escapes embedded double quotes for IMAP quoted-string atoms
// (RFC 3501 §4.3). Used when building LOGIN, STATUS, etc. with arbitrary
// user-supplied strings.
func escapeQ(s string) string {
	return strings.ReplaceAll(s, `"`, `\"`)
}
