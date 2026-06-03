package usp_gmail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// gmailAPIBaseURL is the root of the Gmail REST API. It is overridable through
// config (BaseURL) so tests can point the client at an httptest server.
const gmailAPIBaseURL = "https://gmail.googleapis.com"

// messageRef is one entry of a users.messages.list response: just the ids. The
// full message must be fetched separately with users.messages.get.
type messageRef struct {
	ID       string `json:"id"`
	ThreadID string `json:"threadId"`
}

// listMessagesResponse mirrors the users.messages.list response envelope.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/list
type listMessagesResponse struct {
	Messages           []messageRef `json:"messages"`
	NextPageToken      string       `json:"nextPageToken"`
	ResultSizeEstimate int64        `json:"resultSizeEstimate"`
}

// parseListMessages decodes a users.messages.list response body. An empty body
// or one with no "messages" field (no matches) yields a zero-value response with
// no error.
func parseListMessages(raw []byte) (*listMessagesResponse, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return &listMessagesResponse{}, nil
	}
	var resp listMessagesResponse
	if err := json.Unmarshal([]byte(trimmed), &resp); err != nil {
		return nil, fmt.Errorf("invalid list JSON: %v", err)
	}
	return &resp, nil
}

// GmailClient is a thin wrapper over the Gmail REST API. It owns no token
// lifecycle of its own -- it asks its tokenSource for a bearer token on each
// request and, on a 401, forces a single refresh-and-retry so an access token
// that expired between two polls does not surface as an error.
type GmailClient struct {
	baseURL    string
	userID     string
	ts         *tokenSource
	httpClient *http.Client
}

// NewGmailClient builds a client. baseURL is the API root (e.g.
// "https://gmail.googleapis.com"); userID is the mailbox to read ("me" for the
// authenticated/impersonated user, or an email address).
func NewGmailClient(baseURL, userID string, ts *tokenSource, httpClient *http.Client) *GmailClient {
	return &GmailClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		userID:     userID,
		ts:         ts,
		httpClient: httpClient,
	}
}

// listMessagesParams bundles the query knobs for ListMessages.
type listMessagesParams struct {
	query            string
	pageToken        string
	maxResults       int
	includeSpamTrash bool
	labelIDs         []string
}

// ListMessages issues a users.messages.list request and returns the raw
// response body. The caller parses it (parseListMessages) so the request and
// retry concerns stay separate from decoding.
func (c *GmailClient) ListMessages(ctx context.Context, p listMessagesParams) ([]byte, error) {
	q := url.Values{}
	if p.query != "" {
		q.Set("q", p.query)
	}
	if p.maxResults > 0 {
		q.Set("maxResults", fmt.Sprintf("%d", p.maxResults))
	}
	if p.pageToken != "" {
		q.Set("pageToken", p.pageToken)
	}
	if p.includeSpamTrash {
		q.Set("includeSpamTrash", "true")
	}
	for _, l := range p.labelIDs {
		q.Add("labelIds", l)
	}

	reqURL := fmt.Sprintf("%s/gmail/v1/users/%s/messages", c.baseURL, url.PathEscape(c.userID))
	if encoded := q.Encode(); encoded != "" {
		reqURL += "?" + encoded
	}
	return c.do(ctx, http.MethodGet, reqURL)
}

// GetMessage issues a users.messages.get request for one message id and returns
// the raw response body. format is one of minimal/full/raw/metadata;
// metadataHeaders restricts the headers returned when format is "metadata".
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get
func (c *GmailClient) GetMessage(ctx context.Context, id, format string, metadataHeaders []string) ([]byte, error) {
	q := url.Values{}
	if format != "" {
		q.Set("format", format)
	}
	for _, h := range metadataHeaders {
		q.Add("metadataHeaders", h)
	}

	reqURL := fmt.Sprintf("%s/gmail/v1/users/%s/messages/%s",
		c.baseURL, url.PathEscape(c.userID), url.PathEscape(id))
	if encoded := q.Encode(); encoded != "" {
		reqURL += "?" + encoded
	}
	return c.do(ctx, http.MethodGet, reqURL)
}

// --- BEC capability endpoints ----------------------------------------------
//
// These read the mailbox's configuration state and change history. All are
// readable with the default gmail.readonly scope. Each returns the raw response
// body; the adapter parses and ships it.

// settingsBaseURL is the root of the per-user settings sub-resources.
func (c *GmailClient) settingsBaseURL() string {
	return fmt.Sprintf("%s/gmail/v1/users/%s/settings", c.baseURL, url.PathEscape(c.userID))
}

// ListFilters issues a users.settings.filters.list request.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings.filters/list
func (c *GmailClient) ListFilters(ctx context.Context) ([]byte, error) {
	return c.do(ctx, http.MethodGet, c.settingsBaseURL()+"/filters")
}

// ListForwardingAddresses issues a users.settings.forwardingAddresses.list request.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings.forwardingAddresses/list
func (c *GmailClient) ListForwardingAddresses(ctx context.Context) ([]byte, error) {
	return c.do(ctx, http.MethodGet, c.settingsBaseURL()+"/forwardingAddresses")
}

// GetAutoForwarding issues a users.settings.getAutoForwarding request.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings/getAutoForwarding
func (c *GmailClient) GetAutoForwarding(ctx context.Context) ([]byte, error) {
	return c.do(ctx, http.MethodGet, c.settingsBaseURL()+"/autoForwarding")
}

// ListSendAs issues a users.settings.sendAs.list request.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings.sendAs/list
func (c *GmailClient) ListSendAs(ctx context.Context) ([]byte, error) {
	return c.do(ctx, http.MethodGet, c.settingsBaseURL()+"/sendAs")
}

// ListDelegates issues a users.settings.delegates.list request. Google exposes
// this only to service accounts with domain-wide delegation (Workspace).
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings.delegates/list
func (c *GmailClient) ListDelegates(ctx context.Context) ([]byte, error) {
	return c.do(ctx, http.MethodGet, c.settingsBaseURL()+"/delegates")
}

// GetImap issues a users.settings.getImap request.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings/getImap
func (c *GmailClient) GetImap(ctx context.Context) ([]byte, error) {
	return c.do(ctx, http.MethodGet, c.settingsBaseURL()+"/imap")
}

// GetPop issues a users.settings.getPop request.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings/getPop
func (c *GmailClient) GetPop(ctx context.Context) ([]byte, error) {
	return c.do(ctx, http.MethodGet, c.settingsBaseURL()+"/pop")
}

// GetVacation issues a users.settings.getVacation request.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.settings/getVacation
func (c *GmailClient) GetVacation(ctx context.Context) ([]byte, error) {
	return c.do(ctx, http.MethodGet, c.settingsBaseURL()+"/vacation")
}

// GetProfile issues a users.getProfile request, used to obtain a baseline
// historyId for incremental history collection.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users/getProfile
func (c *GmailClient) GetProfile(ctx context.Context) ([]byte, error) {
	reqURL := fmt.Sprintf("%s/gmail/v1/users/%s/profile", c.baseURL, url.PathEscape(c.userID))
	return c.do(ctx, http.MethodGet, reqURL)
}

// listHistoryParams bundles the query knobs for ListHistory.
type listHistoryParams struct {
	startHistoryID string
	pageToken      string
	maxResults     int
	historyTypes   []string
	labelID        string
}

// ListHistory issues a users.history.list request and returns the raw response
// body.
//
// Reference:
// https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list
func (c *GmailClient) ListHistory(ctx context.Context, p listHistoryParams) ([]byte, error) {
	q := url.Values{}
	if p.startHistoryID != "" {
		q.Set("startHistoryId", p.startHistoryID)
	}
	if p.pageToken != "" {
		q.Set("pageToken", p.pageToken)
	}
	if p.maxResults > 0 {
		q.Set("maxResults", fmt.Sprintf("%d", p.maxResults))
	}
	for _, ht := range p.historyTypes {
		q.Add("historyTypes", ht)
	}
	if p.labelID != "" {
		q.Set("labelId", p.labelID)
	}

	reqURL := fmt.Sprintf("%s/gmail/v1/users/%s/history", c.baseURL, url.PathEscape(c.userID))
	if encoded := q.Encode(); encoded != "" {
		reqURL += "?" + encoded
	}
	return c.do(ctx, http.MethodGet, reqURL)
}

// do executes a request, transparently refreshing the access token once if the
// API answers 401.
func (c *GmailClient) do(ctx context.Context, method, reqURL string) ([]byte, error) {
	body, err := c.doOnce(ctx, method, reqURL, false)
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusUnauthorized {
		return c.doOnce(ctx, method, reqURL, true)
	}
	return body, err
}

func (c *GmailClient) doOnce(ctx context.Context, method, reqURL string, forceToken bool) ([]byte, error) {
	token, err := c.ts.Token(ctx, forceToken)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", reqURL, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request %q: %v", reqURL, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response %q: %v", reqURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, parseHTTPError(resp.StatusCode, reqURL, string(respBody))
	}
	return respBody, nil
}

// Close releases idle connections held by the underlying transport.
func (c *GmailClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// newAPIHTTPClient builds the HTTP client used for Gmail API requests.
func newAPIHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{Timeout: 10 * time.Second}).Dial,
		},
	}
}
