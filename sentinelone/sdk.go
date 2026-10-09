package usp_sentinelone

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPError represents an HTTP error with status code for proper error handling.
// This allows callers to inspect the status code directly without parsing error strings.
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("unexpected status code %d for %q: %s", e.StatusCode, e.URL, e.Body)
}

// SentinelOneClient represents a SentinelOne API SentinelOneClient
type SentinelOneClient struct {
	baseURL               string
	apiToken              string
	httpSentinelOneClient *http.Client
}

// NewSentinelOneClient creates a new SentinelOne API SentinelOneClient
func NewSentinelOneClient(baseURL, apiToken string) *SentinelOneClient {
	return &SentinelOneClient{
		baseURL:               strings.TrimRight(baseURL, "/"),
		apiToken:              apiToken,
		httpSentinelOneClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// SentinelOnePagination is the envelope the Management API nests page state
// under: {"pagination": {"totalItems": N, "nextCursor": "..."}, "data": [...]}.
type SentinelOnePagination struct {
	TotalItems int     `json:"totalItems"`
	NextCursor *string `json:"nextCursor"`
}

// SentinelOnePagedData represents pagination information
type SentinelOnePagedData struct {
	Data       []map[string]interface{} `json:"data"`
	TotalItems int                      `json:"totalItems"`
	NextCursor *string                  `json:"nextCursor"`
	Pagination *SentinelOnePagination   `json:"pagination"`
}

// NextPageCursor returns the cursor for the next page, or "" on the last
// page. The live Management API nests the cursor under "pagination"; the
// top-level "nextCursor" is kept as a fallback for older/other response
// shapes.
func (d *SentinelOnePagedData) NextPageCursor() string {
	if d.Pagination != nil && d.Pagination.NextCursor != nil {
		return *d.Pagination.NextCursor
	}
	if d.NextCursor != nil {
		return *d.NextCursor
	}
	return ""
}

// GetFromAPI retrieves data from the API based on the provided options
func (c *SentinelOneClient) GetFromAPI(ctx context.Context, endpoint string, opts url.Values) (*SentinelOnePagedData, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s%s", c.baseURL, endpoint), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request %q: %v", req.URL.String(), err)
	}

	// Add query parameters
	req.URL.RawQuery = opts.Encode()

	// Add authentication
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpSentinelOneClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request %q: %v", req.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Read the response body for an error message
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body %q: %v", req.URL.String(), err)
		}
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        req.URL.String(),
			Body:       string(body),
		}
	}

	var result SentinelOnePagedData
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response %q: %v", req.URL.String(), err)
	}

	return &result, nil
}
