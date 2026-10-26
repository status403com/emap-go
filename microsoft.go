package emap

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// microsoftLoginBase is the Microsoft identity platform host. Var so tests
// can point it at an httptest server.
var microsoftLoginBase = "https://login.microsoftonline.com"

// microsoftIMAPScope is the delegated permission Outlook / Exchange Online
// IMAP requires, plus offline_access so the token endpoint issues a refresh
// token.
const microsoftIMAPScope = "https://outlook.office.com/IMAP.AccessAsUser.All offline_access"

// accessTokenRefreshMargin is how long before expiry a cached access token
// is considered stale, so a token never expires between Token() and the
// server validating it.
const accessTokenRefreshMargin = 2 * time.Minute

var oauthHTTPClient = &http.Client{Timeout: 30 * time.Second}

// MicrosoftOAuth identifies the Microsoft Entra app registration used to
// obtain IMAP tokens for Outlook.com / Hotmail / Live and Microsoft 365
// mailboxes, which no longer accept password LOGIN.
//
// Typical flow: call StartDeviceLogin once per mailbox, show the returned
// code to the mailbox owner (who signs in through any browser), Wait for the
// refresh token and persist it, then connect with
//
//	emap.Credential{
//		Host: "outlook.office365.com", Port: 993, UseTLS: true,
//		Email:  "someone@hotmail.com",
//		OAuth2: oauth.TokenSource(refreshToken, saveRefreshToken),
//	}
type MicrosoftOAuth struct {
	// ClientID is the Application (client) ID of an app registration that
	// supports the account type being signed in (personal Microsoft
	// accounts for @hotmail/@outlook/@live), has "Allow public client
	// flows" enabled, and lists the Office 365 Exchange Online delegated
	// permission IMAP.AccessAsUser.All.
	ClientID string
	// Tenant selects the authority: "common" (default when empty),
	// "consumers", "organizations", or a specific tenant ID / domain.
	Tenant string
}

// DeviceCode is an in-progress device-code sign-in returned by
// StartDeviceLogin. Display UserCode and VerificationURI (or Message) to the
// mailbox owner, who opens the URI in any browser and enters the code
// before ExpiresAt; then call Wait for the refresh token.
type DeviceCode struct {
	UserCode        string
	VerificationURI string
	// Message is Microsoft's ready-to-display instruction combining the
	// URI and code.
	Message   string
	ExpiresAt time.Time

	oauth      MicrosoftOAuth
	deviceCode string
	interval   time.Duration
}

// StartDeviceLogin begins the OAuth2 device authorization grant and returns
// the code for the mailbox owner to enter. It does not block on the
// owner; call Wait on the result to collect the refresh token.
func (o MicrosoftOAuth) StartDeviceLogin(ctx context.Context) (*DeviceCode, error) {
	var dc struct {
		DeviceCode       string `json:"device_code"`
		UserCode         string `json:"user_code"`
		VerificationURI  string `json:"verification_uri"`
		ExpiresIn        int    `json:"expires_in"`
		Interval         int    `json:"interval"`
		Message          string `json:"message"`
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	err := o.post(ctx, "devicecode", url.Values{
		"client_id": {o.ClientID},
		"scope":     {microsoftIMAPScope},
	}, &dc)
	if err != nil {
		return nil, err
	}
	if dc.Error != "" {
		return nil, oauthError("devicecode", dc.Error, dc.ErrorDescription)
	}
	interval := time.Duration(dc.Interval) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &DeviceCode{
		UserCode:        dc.UserCode,
		VerificationURI: dc.VerificationURI,
		Message:         dc.Message,
		ExpiresAt:       time.Now().Add(time.Duration(dc.ExpiresIn) * time.Second),
		oauth:           o,
		deviceCode:      dc.DeviceCode,
		interval:        interval,
	}, nil
}

// Wait polls Microsoft until the mailbox owner approves, declines, the code
// expires, or ctx is canceled. Returns the refresh token to persist and
// feed to TokenSource. Call at most once per DeviceCode.
func (d *DeviceCode) Wait(ctx context.Context) (string, error) {
	interval := d.interval
	for {
		t := time.NewTimer(interval)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return "", ctx.Err()
		}
		var tr tokenResponse
		err := d.oauth.post(ctx, "token", url.Values{
			"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
			"client_id":   {d.oauth.ClientID},
			"device_code": {d.deviceCode},
		}, &tr)
		if err != nil {
			return "", err
		}
		switch tr.Error {
		case "":
			if tr.RefreshToken == "" {
				return "", fmt.Errorf("microsoft oauth token: no refresh token issued (offline_access not granted)")
			}
			return tr.RefreshToken, nil
		case "authorization_pending":
		case "slow_down":
			interval += 5 * time.Second
		default:
			return "", oauthError("token", tr.Error, tr.ErrorDescription)
		}
	}
}

// TokenSource returns a TokenSource that redeems refreshToken for IMAP
// access tokens, caching each until shortly before it expires.
//
// Microsoft rotates refresh tokens: when a refresh returns a new one,
// onRotate (if non-nil) is called with it synchronously, before Token
// returns. Persist it there — the next process start must use the newest
// refresh token. onRotate must not call Token.
func (o MicrosoftOAuth) TokenSource(refreshToken string, onRotate func(refreshToken string)) TokenSource {
	return &microsoftTokenSource{oauth: o, refreshToken: refreshToken, onRotate: onRotate}
}

type microsoftTokenSource struct {
	oauth    MicrosoftOAuth
	onRotate func(string)

	mu           sync.Mutex
	refreshToken string
	accessToken  string
	expiry       time.Time
}

func (s *microsoftTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accessToken != "" && time.Now().Before(s.expiry) {
		return s.accessToken, nil
	}
	var tr tokenResponse
	err := s.oauth.post(ctx, "token", url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {s.oauth.ClientID},
		"refresh_token": {s.refreshToken},
		"scope":         {microsoftIMAPScope},
	}, &tr)
	if err != nil {
		return "", err
	}
	switch tr.Error {
	case "":
	case "invalid_grant", "interaction_required":
		return "", fmt.Errorf("%w: %w", ErrReauthRequired, oauthError("token", tr.Error, tr.ErrorDescription))
	default:
		return "", oauthError("token", tr.Error, tr.ErrorDescription)
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("microsoft oauth token: response carried no access token")
	}
	s.accessToken = tr.AccessToken
	s.expiry = time.Now().Add(time.Duration(tr.ExpiresIn)*time.Second - accessTokenRefreshMargin)
	if tr.RefreshToken != "" && tr.RefreshToken != s.refreshToken {
		s.refreshToken = tr.RefreshToken
		if s.onRotate != nil {
			s.onRotate(tr.RefreshToken)
		}
	}
	return s.accessToken, nil
}

// tokenResponse covers both success and error bodies from the v2.0 token
// endpoint; errors come back as HTTP 400 with the error fields set.
type tokenResponse struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	ExpiresIn        int    `json:"expires_in"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// post sends a form to the tenant's oauth2/v2.0/<endpoint> and decodes the
// JSON body into out. Error bodies are JSON too, so a non-2xx status is only
// reported when the body isn't decodable.
func (o MicrosoftOAuth) post(ctx context.Context, endpoint string, form url.Values, out any) error {
	tenant := o.Tenant
	if tenant == "" {
		tenant = "common"
	}
	u := microsoftLoginBase + "/" + url.PathEscape(tenant) + "/oauth2/v2.0/" + endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("microsoft oauth %s: %w", endpoint, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := oauthHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("microsoft oauth %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("microsoft oauth %s: read body: %w", endpoint, err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("microsoft oauth %s: HTTP %d: undecodable body", endpoint, resp.StatusCode)
	}
	return nil
}

func oauthError(endpoint, code, desc string) error {
	return fmt.Errorf("microsoft oauth %s: %s: %s", endpoint, code, desc)
}
