package usp_cortex_xdr

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultPageSize         = 100
	defaultTimeBetween      = 1 * time.Minute
	authTypeStandard        = "standard"
	authTypeAdvanced        = "advanced"
	nonceCharset            = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	nonceLen                = 64
	rateLimitRetryDelay     = 60 * time.Second
	consecutiveErrorsCutoff = 10
	incidentsCallName       = "get_incidents"
	alertsCallName          = "get_alerts_multi_events"
	incidentsAPI            = "incidents"
	alertsAPI               = "alerts"
)

type CortexXdrConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	APIKeyID            string                  `json:"api_key_id" yaml:"api_key_id"`
	APIKey              string                  `json:"api_key" yaml:"api_key"`
	APIURL              string                  `json:"api_url" yaml:"api_url"`
	AuthType            string                  `json:"auth_type" yaml:"auth_type"`
	DataTypes           string                  `json:"data_types" yaml:"data_types"`
	StartTime           string                  `json:"start_time" yaml:"start_time"`
	TimeBetweenRequests time.Duration           `json:"time_between_requests" yaml:"time_between_requests"`
}

func (c *CortexXdrConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.APIKey == "" {
		return errors.New("missing api_key")
	}
	if c.APIKeyID == "" {
		return errors.New("missing api_key_id")
	}
	if c.APIURL == "" {
		return errors.New("missing api_url (e.g. https://api-<tenant>.xdr.us.paloaltonetworks.com)")
	}
	if !strings.HasPrefix(c.APIURL, "http://") && !strings.HasPrefix(c.APIURL, "https://") {
		c.APIURL = "https://" + c.APIURL
	}
	c.APIURL = strings.TrimRight(c.APIURL, "/")

	if c.AuthType == "" {
		c.AuthType = authTypeAdvanced
	}
	c.AuthType = strings.ToLower(c.AuthType)
	if c.AuthType != authTypeStandard && c.AuthType != authTypeAdvanced {
		return fmt.Errorf("invalid auth_type %q (must be 'standard' or 'advanced')", c.AuthType)
	}

	if c.StartTime != "" {
		if _, err := time.Parse(time.RFC3339, c.StartTime); err != nil {
			return fmt.Errorf("invalid start_time %q (must be RFC3339): %v", c.StartTime, err)
		}
	}

	if c.TimeBetweenRequests == 0 {
		c.TimeBetweenRequests = defaultTimeBetween
	}
	return nil
}

type CortexXdrAdapter struct {
	conf       CortexXdrConfig
	uspClient  *uspclient.Client
	httpClient *http.Client

	chStopped chan struct{}
	wgSenders sync.WaitGroup
	doStop    *utils.Event

	ctx context.Context
}

type dataType struct {
	name      string
	api       string
	callName  string
	replyKey  string
	timeField string
	idField   string
	eventType string
}

var allDataTypes = map[string]dataType{
	"incidents": {
		name:      "incidents",
		api:       incidentsAPI,
		callName:  incidentsCallName,
		replyKey:  "incidents",
		timeField: "creation_time",
		idField:   "incident_id",
		eventType: "incident",
	},
	"alerts": {
		name:      "alerts",
		api:       alertsAPI,
		callName:  alertsCallName,
		replyKey:  "alerts",
		timeField: "creation_time",
		idField:   "alert_id",
		eventType: "alert",
	},
}

func NewCortexXdrAdapter(ctx context.Context, conf CortexXdrConfig) (*CortexXdrAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	a := &CortexXdrAdapter{
		conf:   conf,
		ctx:    ctx,
		doStop: utils.NewEvent(),
	}

	var err error
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.httpClient = &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	a.chStopped = make(chan struct{})

	selected, err := a.resolveDataTypes()
	if err != nil {
		a.Close()
		return nil, nil, err
	}

	startTime := a.initialStartTime()

	for _, dt := range selected {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting to fetch cortex_xdr %s events", dt.name))
		a.wgSenders.Add(1)
		go a.fetchEvents(dt, startTime)
	}

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *CortexXdrAdapter) resolveDataTypes() ([]dataType, error) {
	requested := a.conf.DataTypes
	if requested == "" {
		// Default: pull both incidents and alerts.
		return []dataType{allDataTypes["incidents"], allDataTypes["alerts"]}, nil
	}
	out := []dataType{}
	for _, name := range strings.Split(requested, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		dt, ok := allDataTypes[name]
		if !ok {
			return nil, fmt.Errorf("unknown data_type %q (valid: incidents, alerts)", name)
		}
		out = append(out, dt)
	}
	if len(out) == 0 {
		return nil, errors.New("no valid data_types specified")
	}
	return out, nil
}

func (a *CortexXdrAdapter) initialStartTime() time.Time {
	if a.conf.StartTime == "" {
		return time.Now().UTC().Add(-1 * time.Hour)
	}
	t, err := time.Parse(time.RFC3339, a.conf.StartTime)
	if err != nil {
		return time.Now().UTC().Add(-1 * time.Hour)
	}
	return t.UTC()
}

func (a *CortexXdrAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.doStop.Set()
	a.wgSenders.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()
	a.httpClient.CloseIdleConnections()

	if err1 != nil {
		return err1
	}
	return err2
}

func (a *CortexXdrAdapter) fetchEvents(dt dataType, startTime time.Time) {
	defer a.wgSenders.Done()
	defer a.conf.ClientOptions.DebugLog(fmt.Sprintf("fetching of cortex_xdr %s events exiting", dt.name))

	// last is the high-water mark on the timeField (in ms since epoch).
	// We always query strictly after this timestamp, advancing it as
	// new events arrive to avoid re-shipping duplicates.
	last := startTime.UnixMilli()
	consecutiveErrors := 0

	for {
		fetched, newLast, err := a.fetchOnce(dt, last)
		if err != nil {
			consecutiveErrors++
			a.conf.ClientOptions.OnError(fmt.Errorf("cortex_xdr %s fetch failed: %v", dt.name, err))
			if consecutiveErrors >= consecutiveErrorsCutoff {
				a.conf.ClientOptions.OnError(fmt.Errorf("cortex_xdr %s: too many consecutive errors, stopping", dt.name))
				a.doStop.Set()
				return
			}
		} else {
			consecutiveErrors = 0
			last = newLast
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("cortex_xdr %s: fetched %d events", dt.name, fetched))
		}

		if a.doStop.WaitFor(a.conf.TimeBetweenRequests) {
			return
		}
	}
}

// fetchOnce paginates through one window of results since `sinceMs` (exclusive)
// and ships them. Returns (count, new high-water mark, error).
func (a *CortexXdrAdapter) fetchOnce(dt dataType, sinceMs int64) (int, int64, error) {
	highWater := sinceMs
	totalShipped := 0
	searchFrom := 0

	for {
		if a.doStop.IsSet() {
			return totalShipped, highWater, nil
		}

		body := map[string]interface{}{
			"request_data": map[string]interface{}{
				"filters": []map[string]interface{}{
					{
						"field":    dt.timeField,
						"operator": "gte",
						"value":    sinceMs + 1,
					},
				},
				"search_from": searchFrom,
				"search_to":   searchFrom + defaultPageSize,
				"sort": map[string]interface{}{
					"field":   dt.timeField,
					"keyword": "asc",
				},
			},
		}

		reply, err := a.callAPI(dt.api, dt.callName, body)
		if err != nil {
			return totalShipped, highWater, err
		}

		items, _ := reply[dt.replyKey].([]interface{})
		resultCount := len(items)

		for _, raw := range items {
			ev, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}

			ts := extractTimestampMs(ev, dt.timeField)
			if ts > highWater {
				highWater = ts
			}

			msg := &protocol.DataMessage{
				EventType:   dt.eventType,
				JsonPayload: ev,
				TimestampMs: uint64(ts),
			}
			if msg.TimestampMs == 0 {
				msg.TimestampMs = uint64(time.Now().UnixMilli())
			}

			if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
				if err == uspclient.ErrorBufferFull {
					a.conf.ClientOptions.OnWarning("stream falling behind")
					err = a.uspClient.Ship(msg, 1*time.Hour)
				}
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
					a.doStop.Set()
					return totalShipped, highWater, nil
				}
			}
			totalShipped++
		}

		if resultCount < defaultPageSize {
			break
		}
		searchFrom += defaultPageSize
	}

	return totalShipped, highWater, nil
}

// callAPI POSTs to the Cortex XDR API and returns the contents of the "reply" field.
// It transparently handles 429 rate limiting with a single retry.
func (a *CortexXdrAdapter) callAPI(api, callName string, body map[string]interface{}) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/public_api/v1/%s/%s/", a.conf.APIURL, api, callName)

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal: %v", err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if a.doStop.IsSet() {
			return nil, errors.New("stopping")
		}

		reqCtx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(jsonBody))
		if err != nil {
			cancel()
			return nil, fmt.Errorf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		for k, v := range a.authHeaders() {
			req.Header.Set(k, v)
		}

		resp, err := a.httpClient.Do(req)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("do: %v", err)
		}
		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		if readErr != nil {
			return nil, fmt.Errorf("read body: %v", readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("cortex_xdr %s/%s got 429, sleeping %s", api, callName, rateLimitRetryDelay))
			if a.doStop.WaitFor(rateLimitRetryDelay) {
				return nil, errors.New("stopping")
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("non-200 from %s: %d %s", url, resp.StatusCode, string(respBody))
		}

		var parsed struct {
			Reply json.RawMessage `json:"reply"`
		}
		if err := json.Unmarshal(respBody, &parsed); err != nil {
			return nil, fmt.Errorf("invalid json: %v", err)
		}
		out := map[string]interface{}{}
		if len(parsed.Reply) > 0 {
			if err := json.Unmarshal(parsed.Reply, &out); err != nil {
				return nil, fmt.Errorf("invalid reply json: %v", err)
			}
		}
		return out, nil
	}
	return nil, errors.New("exhausted retries")
}

// authHeaders builds Cortex XDR API auth headers per the configured auth_type.
// Standard: just identifies the key. Advanced: includes a SHA-256 of
// (key + nonce + timestamp_ms) to prevent replay.
func (a *CortexXdrAdapter) authHeaders() map[string]string {
	if a.conf.AuthType == authTypeStandard {
		return map[string]string{
			"x-xdr-auth-id": a.conf.APIKeyID,
			"Authorization": a.conf.APIKey,
		}
	}
	nonce := randomNonce(nonceLen)
	timestamp := time.Now().UTC().UnixMilli()
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s%s%d", a.conf.APIKey, nonce, timestamp)))
	return map[string]string{
		"x-xdr-timestamp": fmt.Sprintf("%d", timestamp),
		"x-xdr-nonce":     nonce,
		"x-xdr-auth-id":   a.conf.APIKeyID,
		"Authorization":   hex.EncodeToString(sum[:]),
	}
}

func randomNonce(n int) string {
	out := make([]byte, n)
	max := big.NewInt(int64(len(nonceCharset)))
	for i := 0; i < n; i++ {
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			// crypto/rand should not fail on supported platforms; fall back
			// to a deterministic char rather than aborting.
			out[i] = nonceCharset[i%len(nonceCharset)]
			continue
		}
		out[i] = nonceCharset[idx.Int64()]
	}
	return string(out)
}

// extractTimestampMs looks for a numeric millisecond timestamp at the named
// field (Cortex returns these as JSON numbers).
func extractTimestampMs(ev map[string]interface{}, field string) int64 {
	v, ok := ev[field]
	if !ok {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
	}
	return 0
}
