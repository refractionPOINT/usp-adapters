package usp_okta

import (
	"context"
	"fmt"
	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Okta's own status and error body must reach the error; the API key (also
// echoed back in the body) must not.
func TestOktaVendorErrorBodyReachesErrorWithoutAPIKey(t *testing.T) {
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	var status int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"errorCode":"E0000011","errorSummary":"Invalid token provided","errorCauses":[],"echo":"%s"}`, r.Header.Get("Authorization"))
	}))
	defer server.Close()
	a := directAdapter(t, server.URL)
	for code, kind := range map[int]error{401: errVendorCredential, 403: errVendorCredential, 503: errRequestRetry, 404: errInvalidPage} {
		status = code
		_, err := a.requestPage(context.Background(), server.URL+logsURL)
		require.ErrorIs(t, err, kind)
		require.Contains(t, err.Error(), fmt.Sprintf("HTTP %d", code))
		require.Contains(t, err.Error(), `"errorSummary":"Invalid token provided"`)
		require.NotContains(t, err.Error(), a.conf.ApiKey)
		require.Contains(t, err.Error(), "[REDACTED]")
	}
}

// A 403 body that echoes the configured token with an HTML-escaped character
// ("&#95;" for "_") must not leave any part of the token in the OnError report:
// the entity's ";" ends no value, and a short tail is not left behind.
func TestOktaOnErrorHidesEntityEncodedTokenEcho(t *testing.T) {
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	const token = "00aV7Hq9NmJ2kQp6xB4RsY8Lv0tC3nE5dF6_wX7z9A"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, "<html><body>Forbidden. Authorization: SSWS %s</body></html>", strings.ReplaceAll(token, "_", "&#95;"))
	}))
	defer server.Close()
	a, g := localHarness(t, server.URL, &ackSink{})
	a.conf.ApiKey = token
	var reports reportLog
	a.conf.ClientOptions.OnError = reports.add
	stop := runCollector(t, a, g)
	defer stop()
	require.Eventually(t, func() bool { return len(reports.all()) >= 1 }, 10*time.Second, 10*time.Millisecond)
	got := reports.all()[0]
	require.Contains(t, got, "HTTP 403")
	require.Contains(t, got, "Forbidden. Authorization: [REDACTED]")
	for i := 0; i+6 <= len(token); i++ {
		require.NotContains(t, got, token[i:i+6])
	}
}
