package usp_gmail

// This file implements automatic mailbox discovery via the Admin SDK Directory
// API. When enabled, the adapter periodically enumerates the Workspace domain's
// users and reconciles the running set of per-mailbox collectors against it:
// newly-provisioned mailboxes get a collector (and their own sensor), and
// deprovisioned ones are stopped and torn down. Mailboxes named explicitly in
// the config (subject / subjects) are always kept regardless of what discovery
// returns.
//
// Discovery impersonates an administrator (admin_subject) and needs the
// admin.directory.user.readonly scope in the service account's domain-wide
// delegation. The Gmail collection of each discovered mailbox still impersonates
// that mailbox with the Gmail scope.
//
// Reference:
// https://developers.google.com/admin-sdk/directory/reference/rest/v1/users/list

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// directoryAPIBaseURL is the root of the Admin SDK Directory API.
const directoryAPIBaseURL = "https://admin.googleapis.com"

// directoryMaxResults is the page size for the users.list enumeration (the API
// caps this at 500).
const directoryMaxResults = 500

// discoveredUser is the subset of a Directory API user resource we use.
//
// Only `suspended` is consulted for filtering: it is returned under the default
// "basic" projection. (`archived` is not part of the basic projection, so it
// would always read false here and cannot be relied on.)
type discoveredUser struct {
	PrimaryEmail string `json:"primaryEmail"`
	Suspended    bool   `json:"suspended"`
}

// directoryUsersResponse mirrors the users.list response envelope.
type directoryUsersResponse struct {
	Users         []discoveredUser `json:"users"`
	NextPageToken string           `json:"nextPageToken"`
}

// directoryClient is a thin Admin SDK Directory API client for enumerating the
// domain's mailboxes. Like GmailClient, it refreshes its token once on a 401.
type directoryClient struct {
	baseURL    string
	ts         *tokenSource
	httpClient *http.Client
}

// newDirectoryClient builds the Directory API client used for discovery. It
// impersonates the configured admin subject with the directory read-only scope.
func (a *GmailAdapter) newDirectoryClient() (*directoryClient, error) {
	ts := newServiceAccountSource(
		a.saKey, a.saRSA, a.conf.AdminSubject,
		[]string{adminDirectoryUserReadonlyScope}, a.tokenURL, a.authHTTP)
	return &directoryClient{
		baseURL:    resolveDirectoryBaseURL(a.conf),
		ts:         ts,
		httpClient: newAPIHTTPClient(),
	}, nil
}

func (d *directoryClient) Close() {
	d.ts.Close()
	d.httpClient.CloseIdleConnections()
}

// listUsersParams bundles the query knobs for one users.list page.
type listUsersParams struct {
	customer  string
	domain    string
	query     string
	pageToken string
}

// listUsers issues one users.list request and returns the parsed page.
func (d *directoryClient) listUsers(ctx context.Context, p listUsersParams) (*directoryUsersResponse, error) {
	q := url.Values{}
	if p.domain != "" {
		q.Set("domain", p.domain)
	} else {
		q.Set("customer", p.customer)
	}
	if p.query != "" {
		q.Set("query", p.query)
	}
	q.Set("maxResults", fmt.Sprintf("%d", directoryMaxResults))
	if p.pageToken != "" {
		q.Set("pageToken", p.pageToken)
	}

	reqURL := d.baseURL + "/admin/directory/v1/users?" + q.Encode()
	raw, err := d.get(ctx, reqURL)
	if err != nil {
		return nil, err
	}
	var resp directoryUsersResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("invalid directory users JSON: %v", err)
	}
	return &resp, nil
}

// get executes a GET, transparently refreshing the access token once on a 401.
func (d *directoryClient) get(ctx context.Context, reqURL string) ([]byte, error) {
	body, err := d.getOnce(ctx, reqURL, false)
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusUnauthorized {
		return d.getOnce(ctx, reqURL, true)
	}
	return body, err
}

func (d *directoryClient) getOnce(ctx context.Context, reqURL string, forceToken bool) ([]byte, error) {
	token, err := d.ts.Token(ctx, forceToken)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", reqURL, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := d.httpClient.Do(req)
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

// enumerate walks every page of the domain's users and returns the canonicalized
// mailbox addresses to collect, skipping suspended accounts (unless
// IncludeSuspended). `complete` is false if the page cap was hit before the
// listing ended, i.e. the result is a truncated view of the domain -- callers
// must not treat a truncated result as authoritative for removals.
func (d *directoryClient) enumerate(ctx context.Context, conf GmailConfig) (emails []string, complete bool, err error) {
	seen := map[string]bool{}
	pageToken := ""
	for page := 0; page < maxPagesPerPoll; page++ {
		resp, lerr := d.listUsers(ctx, listUsersParams{
			customer:  conf.Customer,
			domain:    conf.Domain,
			query:     conf.DiscoveryQuery,
			pageToken: pageToken,
		})
		if lerr != nil {
			return nil, false, lerr
		}
		for _, u := range resp.Users {
			if u.Suspended && !conf.IncludeSuspended {
				continue
			}
			email := canonicalMailbox(u.PrimaryEmail)
			if email == "" || seen[email] {
				continue
			}
			seen[email] = true
			emails = append(emails, email)
		}
		if resp.NextPageToken == "" {
			return emails, true, nil
		}
		pageToken = resp.NextPageToken
	}
	// Loop exited on the page cap, not an empty nextPageToken: the view is partial.
	return emails, false, nil
}

// runDiscovery enumerates the domain immediately, then re-enumerates every
// DiscoveryInterval, reconciling the running collectors against the result. It
// runs as a sender goroutine and exits when the adapter is closed.
func (a *GmailAdapter) runDiscovery() {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog("gmail: discovery stopped")

	isFirstRun := true
	for isFirstRun || !a.doStop.WaitFor(a.conf.DiscoveryInterval) {
		isFirstRun = false
		a.discoverOnce()
		if a.doStop.IsSet() {
			return
		}
	}
}

// discoverOnce performs one enumeration + reconciliation pass. Errors are logged
// and the pass is abandoned; the static mailboxes keep collecting regardless.
func (a *GmailAdapter) discoverOnce() {
	emails, complete, err := a.directory.enumerate(a.apiRootCtx, a.conf)
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("gmail: mailbox discovery failed, keeping the current set: %v", err))
		return
	}

	desired := make(map[string]bool, len(emails)+len(a.staticAddrs))
	for addr := range a.staticAddrs {
		desired[addr] = true
	}
	for _, e := range emails {
		desired[e] = true
	}

	// Start collectors for newly-discovered mailboxes.
	nAdded := 0
	for _, e := range emails {
		if a.staticAddrs[e] {
			continue
		}
		a.mu.Lock()
		_, running := a.collectors[e]
		a.mu.Unlock()
		if running {
			continue
		}
		if err := a.startCollector(mailboxTarget{address: e, subject: e, userID: defaultUserID}, 0); err != nil {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("gmail: could not start collector for discovered mailbox %q: %v", e, err))
			continue
		}
		nAdded++
	}

	// Determine which active mailboxes are no longer present (never the static
	// ones, which are always in `desired`).
	var stale []string
	for addr := range a.activeMailboxes() {
		if !desired[addr] {
			stale = append(stale, addr)
		}
	}

	// Guard against mass teardown on a suspicious enumeration. A truncated view
	// (page cap hit) is not authoritative, and an empty result while we are
	// already collecting discovered mailboxes is far more likely a misconfigured
	// query or a transient API state than the whole domain vanishing. In those
	// cases, keep the current set and try again next interval.
	if len(stale) > 0 && (!complete || len(emails) == 0) {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf(
			"gmail: discovery returned a %s result (%d mailboxes); keeping %d existing collector(s) rather than removing them",
			map[bool]string{true: "truncated", false: "complete"}[!complete], len(emails), len(stale)))
		stale = nil
	}

	for _, addr := range stale {
		a.removeCollector(addr)
	}

	a.conf.ClientOptions.DebugLog(fmt.Sprintf(
		"gmail: discovery pass complete (discovered=%d added=%d removed=%d active=%d)",
		len(emails), nAdded, len(stale), len(a.activeMailboxes())))
}

// resolveDirectoryBaseURL computes the Directory API root from the config.
func resolveDirectoryBaseURL(conf GmailConfig) string {
	if conf.DirectoryBaseURL != "" {
		return strings.TrimRight(conf.DirectoryBaseURL, "/")
	}
	return directoryAPIBaseURL
}
