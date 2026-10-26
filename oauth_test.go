package emap

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type staticToken string

func (t staticToken) Token(context.Context) (string, error) { return string(t), nil }

// scriptedSource serves "tok-1" on the first call (the initial connect) and
// defers every later call (reconnects) to after.
type scriptedSource struct {
	calls atomic.Int32
	after func(ctx context.Context) (string, error)
}

func (s *scriptedSource) Token(ctx context.Context) (string, error) {
	if s.calls.Add(1) == 1 {
		return "tok-1", nil
	}
	return s.after(ctx)
}

// xoauth2Script accepts AUTHENTICATE XOAUTH2 only when the client's SASL
// payload matches want. LOGIN is rejected so a test can't pass by falling
// back to password auth. On a bad payload it behaves like Gmail/Exchange:
// a `+ <base64 json>` error challenge that the client must answer with an
// empty line before the tagged NO arrives.
func xoauth2Script(want string, got *atomic.Value) func(*fakeConn) {
	return func(fc *fakeConn) {
		_ = fc.write("* OK fake imap ready\r\n")
		for {
			tag, rest, err := fc.readCommand()
			if err != nil {
				return
			}
			up := strings.ToUpper(rest)
			switch {
			case up == "AUTHENTICATE XOAUTH2":
				_ = fc.write("+ \r\n")
				payload, _, err := fc.readCommand()
				if err != nil {
					return
				}
				raw, _ := base64.StdEncoding.DecodeString(payload)
				got.Store(string(raw))
				if string(raw) == want {
					_ = fc.write(tag + " OK AUTHENTICATE completed\r\n")
					continue
				}
				challenge := base64.StdEncoding.EncodeToString([]byte(`{"status":"401","schemes":"bearer"}`))
				_ = fc.write("+ " + challenge + "\r\n")
				if empty, _, err := fc.readCommand(); err != nil || empty != "" {
					return // client didn't send the required empty response
				}
				_ = fc.write(tag + " NO AUTHENTICATE failed\r\n")
				return
			case strings.HasPrefix(up, "LOGIN "):
				_ = fc.write(tag + " NO Basic authentication is disabled\r\n")
				return
			case up == "CAPABILITY":
				_ = fc.write("* CAPABILITY IMAP4rev1 AUTH=XOAUTH2 IDLE\r\n")
				_ = fc.write(tag + " OK CAPABILITY completed\r\n")
			case strings.HasPrefix(up, "SELECT "):
				_ = fc.write("* OK [UIDNEXT 13] mailbox ready\r\n")
				_ = fc.write(tag + " OK [READ-WRITE] SELECT completed\r\n")
			case up == "IDLE":
				_ = fc.write("+ idling\r\n")
				if _, _, err := fc.readCommand(); err != nil {
					return
				}
				_ = fc.write(tag + " OK IDLE terminated\r\n")
			case up == "LOGOUT":
				_ = fc.write(tag + " OK LOGOUT completed\r\n")
				return
			default:
				_ = fc.write(tag + " BAD unknown\r\n")
			}
		}
	}
}

func oauthCred(token string) Credential {
	c := sampleCred("alice@example.com")
	c.Password = ""
	c.OAuth2 = staticToken(token)
	return c
}

func TestXOAuth2SubscribeAuthenticatesWithBearerToken(t *testing.T) {
	var got atomic.Value
	srv := newFakeServer()
	srv.script = xoauth2Script("user=alice@example.com\x01auth=Bearer tok-1\x01\x01", &got)
	m := NewManager(50 * time.Millisecond).withDial(srv.dialer())
	defer m.Shutdown()

	sub, err := m.Subscribe(oauthCred("tok-1"), Filter{})
	if err != nil {
		t.Fatalf("Subscribe: %v (server saw payload %q)", err, got.Load())
	}
	sub.Close()
}

func TestXOAuth2RejectionAnswersChallengeAndFails(t *testing.T) {
	// A client that never answers the error challenge gets no tagged NO:
	// the fake server gives up after its 2s read deadline and hangs up, so
	// the error-text check below fails within ~2s.
	var got atomic.Value
	srv := newFakeServer()
	srv.script = xoauth2Script("user=alice@example.com\x01auth=Bearer good\x01\x01", &got)
	m := NewManager(50 * time.Millisecond).withDial(srv.dialer())
	defer m.Shutdown()

	_, err := m.Subscribe(oauthCred("expired"), Filter{})
	if err == nil || !strings.Contains(err.Error(), "AUTHENTICATE XOAUTH2 failed") {
		t.Fatalf("expected tagged NO surfaced, got: %v", err)
	}
	if n := m.activeSessions(); n != 0 {
		t.Fatalf("expected 0 sessions after auth failure, got %d", n)
	}
}

func TestReauthOnReconnectEvictsSessionAndClosesSubs(t *testing.T) {
	prev := ReconnectInitialBackoff
	ReconnectInitialBackoff = 10 * time.Millisecond
	defer func() { ReconnectInitialBackoff = prev }()

	var got atomic.Value
	srv := newFakeServer()
	srv.script = xoauth2Script("user=alice@example.com\x01auth=Bearer tok-1\x01\x01", &got)
	m := NewManager(time.Minute).withDial(srv.dialer())
	defer m.Shutdown()

	revoked := &scriptedSource{after: func(context.Context) (string, error) {
		return "", fmt.Errorf("%w: invalid_grant", ErrReauthRequired)
	}}
	cred := sampleCred("alice@example.com")
	cred.OAuth2 = revoked
	sub, err := m.Subscribe(cred, Filter{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close() // after eviction: must be a harmless no-op

	srv.dropConn() // force a reconnect, which now needs a new token
	select {
	case _, ok := <-sub.Ch:
		if ok {
			t.Fatal("unexpected message")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscription still open after sign-in became required")
	}

	var re *ReauthRequiredError
	if !errors.As(sub.Err(), &re) || re.Email != "alice@example.com" || re.Host != cred.Host || re.Port != cred.Port {
		t.Fatalf("sub.Err() = %v, want *ReauthRequiredError for alice@example.com", sub.Err())
	}
	if !errors.Is(sub.Err(), ErrReauthRequired) {
		t.Fatalf("sub.Err() = %v does not match ErrReauthRequired", sub.Err())
	}
	// Several backoff periods later: no further attempts on the dead grant.
	time.Sleep(50 * time.Millisecond)
	if n := revoked.calls.Load(); n != 2 {
		t.Fatalf("token source called %d times, want 2 (connect + one reconnect, no retries)", n)
	}

	// After the owner signs in again, Subscribe must build a fresh session
	// around the new token source rather than join the evicted one.
	fresh := &scriptedSource{}
	cred.OAuth2 = fresh
	sub2, err := m.Subscribe(cred, Filter{})
	if err != nil {
		t.Fatalf("Subscribe with new token source: %v", err)
	}
	sub2.Close()
	if n := fresh.calls.Load(); n != 1 {
		t.Fatalf("new token source called %d times, want 1", n)
	}
}

func TestShutdownCancelsInFlightTokenRefresh(t *testing.T) {
	prev := ReconnectInitialBackoff
	ReconnectInitialBackoff = 10 * time.Millisecond
	defer func() { ReconnectInitialBackoff = prev }()

	var got atomic.Value
	srv := newFakeServer()
	srv.script = xoauth2Script("user=alice@example.com\x01auth=Bearer tok-1\x01\x01", &got)
	m := NewManager(time.Minute).withDial(srv.dialer())

	inRefresh := make(chan struct{})
	var once sync.Once
	cred := sampleCred("alice@example.com")
	cred.OAuth2 = &scriptedSource{after: func(ctx context.Context) (string, error) {
		once.Do(func() { close(inRefresh) })
		<-ctx.Done() // a refresh that only ends when canceled
		return "", ctx.Err()
	}}
	sub, err := m.Subscribe(cred, Filter{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	srv.dropConn()
	select {
	case <-inRefresh:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect never requested a token")
	}
	done := make(chan struct{})
	go func() { m.Shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown blocked behind an in-flight token refresh")
	}
}

// fakeMicrosoft serves /{tenant}/oauth2/v2.0/{devicecode,token} from a
// handler and points microsoftLoginBase at it for the test's duration.
func fakeMicrosoft(t *testing.T, h func(w http.ResponseWriter, path string, form map[string]string)) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		form := map[string]string{}
		for k := range r.PostForm {
			form[k] = r.PostForm.Get(k)
		}
		w.Header().Set("Content-Type", "application/json")
		h(w, r.URL.Path, form)
	}))
	t.Cleanup(ts.Close)
	prev := microsoftLoginBase
	microsoftLoginBase = ts.URL
	t.Cleanup(func() { microsoftLoginBase = prev })
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func TestMicrosoftTokenSourceCachesAndUsesRotatedRefreshToken(t *testing.T) {
	var mu sync.Mutex
	var seen []string // refresh_token values presented, in order
	fakeMicrosoft(t, func(w http.ResponseWriter, path string, form map[string]string) {
		if path != "/consumers/oauth2/v2.0/token" || form["grant_type"] != "refresh_token" || form["client_id"] != "cid" {
			writeJSON(w, 400, map[string]string{"error": "invalid_request", "error_description": path})
			return
		}
		mu.Lock()
		seen = append(seen, form["refresh_token"])
		n := len(seen)
		mu.Unlock()
		writeJSON(w, 200, map[string]any{
			"access_token":  "at-" + strconv.Itoa(n),
			"refresh_token": "rt-" + strconv.Itoa(n),
			"expires_in":    3600,
		})
	})

	var rotated []string
	ts := MicrosoftOAuth{ClientID: "cid", Tenant: "consumers"}.
		TokenSource("rt-0", func(rt string) { rotated = append(rotated, rt) })

	for i := range 2 {
		tok, err := ts.Token(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if tok != "at-1" {
			t.Fatalf("call %d: token = %q, want cached at-1", i, tok)
		}
	}

	// Expire the cached access token; the next refresh must present the
	// rotated refresh token, not the original one.
	src := ts.(*microsoftTokenSource)
	src.mu.Lock()
	src.expiry = time.Now().Add(-time.Second)
	src.mu.Unlock()
	if tok, err := ts.Token(context.Background()); err != nil || tok != "at-2" {
		t.Fatalf("after expiry: token=%q err=%v, want at-2", tok, err)
	}

	if want := []string{"rt-0", "rt-1"}; strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("refresh tokens presented = %v, want %v", seen, want)
	}
	if want := []string{"rt-1", "rt-2"}; strings.Join(rotated, ",") != strings.Join(want, ",") {
		t.Fatalf("onRotate calls = %v, want %v", rotated, want)
	}
}

func TestMicrosoftTokenSourceReportsReauthRequired(t *testing.T) {
	for _, tc := range []struct {
		code   string
		reauth bool
	}{
		{"invalid_grant", true},        // refresh token expired / revoked
		{"interaction_required", true}, // MFA or consent now demanded
		{"temporarily_unavailable", false},
	} {
		t.Run(tc.code, func(t *testing.T) {
			fakeMicrosoft(t, func(w http.ResponseWriter, path string, form map[string]string) {
				writeJSON(w, 400, map[string]string{"error": tc.code, "error_description": "AADSTS: test"})
			})
			// Through Subscribe so the sentinel is proven to survive the
			// session's error wrapping, which is where host apps see it.
			srv := newFakeServer()
			m := NewManager(50 * time.Millisecond).withDial(srv.dialer())
			defer m.Shutdown()
			cred := sampleCred("alice@example.com")
			cred.OAuth2 = MicrosoftOAuth{ClientID: "cid"}.TokenSource("rt", nil)

			_, err := m.Subscribe(cred, Filter{})
			if err == nil || !strings.Contains(err.Error(), tc.code) {
				t.Fatalf("expected %s error, got %v", tc.code, err)
			}
			if got := errors.Is(err, ErrReauthRequired); got != tc.reauth {
				t.Fatalf("errors.Is(err, ErrReauthRequired) = %v, want %v (err: %v)", got, tc.reauth, err)
			}
			var re *ReauthRequiredError
			if tc.reauth && (!errors.As(err, &re) || re.Email != "alice@example.com") {
				t.Fatalf("err = %v, want *ReauthRequiredError naming alice@example.com", err)
			}
		})
	}
}

func TestMicrosoftDeviceLoginPollsUntilApproved(t *testing.T) {
	var polls atomic.Int32
	fakeMicrosoft(t, func(w http.ResponseWriter, path string, form map[string]string) {
		switch path {
		case "/common/oauth2/v2.0/devicecode":
			if form["scope"] != microsoftIMAPScope {
				writeJSON(w, 400, map[string]string{"error": "invalid_scope"})
				return
			}
			writeJSON(w, 200, map[string]any{
				"device_code": "dc", "user_code": "ABC-123",
				"verification_uri": "https://microsoft.com/devicelogin",
				"expires_in":       900, "interval": 1, "message": "go sign in",
			})
		case "/common/oauth2/v2.0/token":
			if form["device_code"] != "dc" {
				writeJSON(w, 400, map[string]string{"error": "bad_verification_code"})
				return
			}
			if polls.Add(1) == 1 {
				writeJSON(w, 400, map[string]string{"error": "authorization_pending"})
				return
			}
			writeJSON(w, 200, map[string]any{"access_token": "at", "refresh_token": "rt", "expires_in": 3600})
		}
	})

	dc, err := MicrosoftOAuth{ClientID: "cid"}.StartDeviceLogin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if dc.UserCode != "ABC-123" || dc.VerificationURI != "https://microsoft.com/devicelogin" || dc.Message != "go sign in" {
		t.Fatalf("StartDeviceLogin returned %+v", dc)
	}
	// The code must be in the caller's hands before any token polling.
	if n := polls.Load(); n != 0 {
		t.Fatalf("StartDeviceLogin polled the token endpoint %d times; it must return the code first", n)
	}
	rt, err := dc.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rt != "rt" {
		t.Fatalf("refresh token = %q, want rt", rt)
	}
	if n := polls.Load(); n != 2 {
		t.Fatalf("polls = %d, want 2 (pending, then approved)", n)
	}
}

func TestMicrosoftDeviceLoginStopsOnDecline(t *testing.T) {
	fakeMicrosoft(t, func(w http.ResponseWriter, path string, form map[string]string) {
		if strings.HasSuffix(path, "/devicecode") {
			writeJSON(w, 200, map[string]any{"device_code": "dc", "user_code": "X", "expires_in": 900, "interval": 1})
			return
		}
		writeJSON(w, 400, map[string]string{"error": "authorization_declined", "error_description": "user said no"})
	})
	dc, err := MicrosoftOAuth{ClientID: "cid"}.StartDeviceLogin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = dc.Wait(context.Background())
	if err == nil || !strings.Contains(err.Error(), "authorization_declined") {
		t.Fatalf("expected authorization_declined, got %v", err)
	}
}
