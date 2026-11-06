package usp_okta

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// Okta's own status and error body must reach the error; the API key (also
// echoed back in the body) must not.
func TestOktaVendorErrorBodyReachesErrorWithoutAPIKey(t *testing.T) {
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
