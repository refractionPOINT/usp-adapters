package usp_entraid

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
	"github.com/stretchr/testify/require"
)

// The Graph/token endpoint's own status and JSON body must reach the error,
// while the configured client secret and bearer token echoed in it must not.
func TestEntraVendorErrorBodyReachesErrorWithoutSecrets(t *testing.T) {
	// A slow -race machine must not cut the body at the production deadline;
	// the server closes it, so this does not slow the test down.
	defer vendorerr.SetBodyTimeoutsForTest(10*time.Second, 10*time.Second)()
	var status int
	var a *EntraIDAdapter
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":{"code":"Authorization_RequestDenied","message":"Insufficient privileges to complete the operation.","echo":"%s %s"}}`,
			a.conf.ClientSecret, r.Header.Get("Authorization"))
	}))
	defer server.Close()
	a, _ = harness(t, server, &captureSink{})
	form := url.Values{"client_secret": {a.conf.ClientSecret}}

	for _, c := range []struct {
		method, bearer string
		status         int
		kind           error
	}{
		{http.MethodGet, "live-bearer-token-value", 403, errVendor},
		{http.MethodPost, "", 400, errVendor},
		{http.MethodGet, "live-bearer-token-value", 503, errRetry},
	} {
		status = c.status
		_, err := a.doJSON(context.Background(), c.method, server.URL, form.Encode(), c.bearer, &graphPage{})
		require.ErrorIs(t, err, c.kind)
		require.Contains(t, err.Error(), fmt.Sprintf("HTTP %d", c.status))
		require.Contains(t, err.Error(), "Authorization_RequestDenied")
		require.Contains(t, err.Error(), "Insufficient privileges to complete the operation.")
		require.NotContains(t, err.Error(), a.conf.ClientSecret)
		require.NotContains(t, err.Error(), "live-bearer-token-value")
	}
}
