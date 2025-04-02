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

// SentinelOnePagedData represents pagination information
type SentinelOnePagedData struct {
	Data       []map[string]interface{} `json:"data"`
	TotalItems int                      `json:"totalItems"`
	NextCursor *string                  `json:"nextCursor"`
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
		return nil, fmt.Errorf("unexpected status code %d for %q: %s", resp.StatusCode, req.URL.String(), string(body))
	}

	var result SentinelOnePagedData
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response %q: %v", req.URL.String(), err)
	}

	return &result, nil
}
