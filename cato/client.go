package usp_cato

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
	"golang.org/x/net/context/ctxhttp"
)

const (
	defaultWriteTimeout = 60 * 10
	// defaultGraphqlURL is the Cato Networks GraphQL endpoint used when the
	// config does not override it.
	defaultGraphqlURL = "https://api.catonetworks.com/api/v1/graphql2"
)

var (
	apiCallCount           int
	totalBytesCompressed   int
	totalBytesUncompressed int
	start                  time.Time
	marker                 string
	configFile             string = "./config.txt"
	// pollInterval is how long the feed loop waits between eventsFeed calls.
	// It is a package variable (not a constant) only so tests can shorten it.
	pollInterval = 1 * time.Minute
	// retryBaseDelay and retryMaxDelay bound the wait between attempts of a
	// request failing in transport (e.g. a TLS error): it doubles per attempt
	// up to the max. Variables only so tests can shorten them.
	retryBaseDelay = 2 * time.Second
	retryMaxDelay  = 5 * time.Minute
	// apiErrorRetryDelay is the wait before re-sending a query the API
	// answered with errors.
	apiErrorRetryDelay = 15 * time.Second
)

const (
	// retryEscalateAfter is the attempt at which a request still failing is
	// reported as an error rather than a warning. Retries continue after it.
	retryEscalateAfter = 10
	// queryAttempts is how many times one eventsFeed query is sent before the
	// cycle is skipped.
	queryAttempts = 3
)

type CatoConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	ApiKey          string                  `json:"apikey" yaml:"apikey"`
	AccountId       int                     `json:"accountid" yaml:"accountid"`
	// Url optionally overrides the Cato GraphQL endpoint. Empty means the
	// production endpoint (defaultGraphqlURL).
	Url string `json:"url,omitempty" yaml:"url,omitempty"`
}

func (c *CatoConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccountId == 0 {
		return errors.New("missing account id")
	}
	if c.ApiKey == "" {
		return errors.New("missing api key")
	}
	return nil
}

// uspSink is the subset of *uspclient.Client the adapter depends on. Expressing
// it as an interface lets tests substitute an in-memory sink for the real
// LimaCharlie client; *uspclient.Client satisfies it unchanged.
type uspSink interface {
	Ship(message *protocol.DataMessage, timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
	Drain(time.Duration) error
}

type CatoAdapter struct {
	conf         CatoConfig
	wg           sync.WaitGroup
	isRunning    uint32
	mRunning     sync.RWMutex
	uspClient    uspSink
	writeTimeout time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx context.Context
}

func NewCatoAdapter(ctx context.Context, conf CatoConfig) (*CatoAdapter, chan struct{}, error) {
	return newCatoAdapter(ctx, conf, nil)
}

// newCatoAdapter is the implementation behind NewCatoAdapter. When sink is
// non-nil it is used in place of a real LimaCharlie client -- the seam tests
// use to capture shipped events.
func newCatoAdapter(ctx context.Context, conf CatoConfig, sink uspSink) (*CatoAdapter, chan struct{}, error) {
	a := &CatoAdapter{
		conf:      conf,
		isRunning: 1,
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	if sink != nil {
		a.uspClient = sink
	} else {
		uspClient, err := uspclient.NewClient(ctx, conf.ClientOptions)
		if err != nil {
			return nil, nil, err
		}
		a.uspClient = uspClient
	}

	a.chStopped = make(chan struct{})

	a.wgSenders.Add(1)
	if marker == "" {
		if _, err := os.Stat(configFile); err == nil {
			content, err := ioutil.ReadFile(configFile)
			if err != nil {
				a.conf.ClientOptions.DebugLog(fmt.Sprintf("Error reading config file: %v", err))
			} else {
				marker = strings.TrimSpace(string(content))
			}
		}
	}

	go a.handleEvent(&marker, strconv.Itoa(a.conf.AccountId), a.conf.ApiKey)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *CatoAdapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")

	a.mRunning.Lock()
	a.isRunning = 0
	a.mRunning.Unlock()

	a.wg.Done()
	a.wg.Wait()

	_, err := utils.DrainCloseUSP(a.uspClient, a.conf.ClientOptions)
	if err != nil {
		return err
	}
	return nil
}

func (a *CatoAdapter) convertStructToMap(obj interface{}) map[string]interface{} {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil
	}

	var mapRepresentation map[string]interface{}
	err = json.Unmarshal(data, &mapRepresentation)
	if err != nil {
		a.conf.ClientOptions.DebugLog(err.Error())
		return nil
	}

	return mapRepresentation
}

func (a *CatoAdapter) handleEvent(marker *string, account_id string, api_key string) {
	defer a.wgSenders.Done()

	start = time.Now()

	eventFilterString := ""
	eventSubfilterString := ""

	iteration := 1
	totalCount := 0

	for {
		// Exit if the adapter was asked to stop (test seam; in production
		// nothing flips isRunning before the process exits).
		a.mRunning.RLock()
		running := a.isRunning
		a.mRunning.RUnlock()
		if running == 0 {
			return
		}

		query := fmt.Sprintf(`{
  eventsFeed(accountIDs:[%q]
    marker:%q
    filters:[%s,%s])
  {
    marker
    fetchedCount
    accounts {
      id
      records {
        time
        fieldsMap
      }
    }
  }
}`, account_id, *marker, eventFilterString, eventSubfilterString)

		var success bool
		var resp map[string]interface{}

		attempts := 0
		for attempts < queryAttempts {
			success, resp = a.send(query, account_id, api_key)
			if success || resp == nil {
				break
			}
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("%s", resp))
			attempts++
			if attempts < queryAttempts && !a.waitRunning(apiErrorRetryDelay) {
				return
			}
		}
		if !success {
			if resp == nil {
				// send() gave up because the adapter is stopping.
				return
			}
			a.conf.ClientOptions.OnError(fmt.Errorf("cato eventsFeed failed after %d attempts, retrying next poll: %v", attempts, resp["errors"]))
			if !a.waitRunning(pollInterval) {
				return
			}
			continue
		}

		*marker = resp["data"].(map[string]interface{})["eventsFeed"].(map[string]interface{})["marker"].(string)
		fetchedCount := int(resp["data"].(map[string]interface{})["eventsFeed"].(map[string]interface{})["fetchedCount"].(float64))
		totalCount += fetchedCount

		line := fmt.Sprintf("iteration:%d fetched:%d total_count:%d marker:%s", iteration, fetchedCount, totalCount, *marker)

		records := resp["data"].(map[string]interface{})["eventsFeed"].(map[string]interface{})["accounts"].([]interface{})[0].(map[string]interface{})["records"].([]interface{})
		if len(records) > 0 {
			line += " " + records[0].(map[string]interface{})["time"].(string)
			line += " " + records[len(records)-1].(map[string]interface{})["time"].(string)
		}

		eventsList := make([]map[string]interface{}, 0)
		for _, event := range records {
			fieldsMap := event.(map[string]interface{})["fieldsMap"].(map[string]interface{})
			fieldsMap["event_timestamp"] = event.(map[string]interface{})["time"].(string)
			eventReorder := make(map[string]interface{})
			for k, v := range fieldsMap {
				if k == "event_timestamp" {
					eventReorder[k] = v
					break
				}
			}
			for k, v := range fieldsMap {
				if k != "event_timestamp" {
					eventReorder[k] = v
				}
			}
			eventsList = append(eventsList, eventReorder)
		}

		for _, event := range eventsList {

			msg := &protocol.DataMessage{
				JsonPayload: a.convertStructToMap(event),
				TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			}
			err := a.uspClient.Ship(msg, a.writeTimeout)
			if err == uspclient.ErrorBufferFull {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = a.uspClient.Ship(msg, 0)
			}
			if err != nil {
				if err != uspclient.ErrorClosed {
					a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
				}
			}
		}

		if err := ioutil.WriteFile(configFile, []byte(*marker), 0644); err != nil {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("Error writing config file: %v", err))
		}

		iteration++

		// Add sleep to ensure the request is made every 1 minute
		time.Sleep(pollInterval)
	}

}

// graphqlURL returns the Cato GraphQL endpoint to call: the config override
// when set, the production endpoint otherwise.
func (a *CatoAdapter) graphqlURL() string {
	if a.conf.Url != "" {
		return a.conf.Url
	}
	return defaultGraphqlURL
}

// retry reports a failed attempt and waits before the next one, longer after
// each consecutive failure. It returns false when the adapter is stopping.
func (a *CatoAdapter) retry(retryCount *int, err error) bool {
	*retryCount++
	delay := retryDelay(*retryCount)
	msg := fmt.Sprintf("cato request failed (attempt %d), retrying in %v: %v", *retryCount, delay, err)
	if *retryCount == retryEscalateAfter {
		a.conf.ClientOptions.OnError(errors.New(msg))
	} else {
		a.conf.ClientOptions.OnWarning(msg)
	}
	return a.waitRunning(delay)
}

// retryDelay is the wait after the given number of consecutive failures.
func retryDelay(failures int) time.Duration {
	delay := retryBaseDelay
	for i := 1; i < failures && delay < retryMaxDelay; i++ {
		delay *= 2
	}
	if delay > retryMaxDelay {
		delay = retryMaxDelay
	}
	return delay
}

// waitRunning sleeps for d, returning early with false if the adapter is
// asked to stop.
func (a *CatoAdapter) waitRunning(d time.Duration) bool {
	const slice = 100 * time.Millisecond
	for deadline := time.Now().Add(d); ; {
		a.mRunning.RLock()
		running := a.isRunning
		a.mRunning.RUnlock()
		if running == 0 {
			return false
		}
		left := time.Until(deadline)
		if left <= 0 {
			return true
		}
		if left > slice {
			left = slice
		}
		time.Sleep(left)
	}
}

func (a *CatoAdapter) send(query string, account_id string, api_key string) (bool, map[string]interface{}) {
	retryCount := 0
	data := map[string]string{"query": query}
	headers := map[string]string{
		"x-api-key":       api_key,
		"Content-Type":    "application/json",
		"Accept-Encoding": "gzip, deflate, br",
	}

	for {
		jsonData, err := json.Marshal(data)
		if err != nil {
			if !a.retry(&retryCount, err) {
				return false, nil
			}
			continue
		}

		req, err := http.NewRequest("POST", a.graphqlURL(), bytes.NewBuffer(jsonData))
		if err != nil {
			if !a.retry(&retryCount, err) {
				return false, nil
			}
			continue
		}

		for key, value := range headers {
			req.Header.Set(key, value)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resp, err := ctxhttp.Do(ctx, http.DefaultClient, req)
		if err != nil {
			if !a.retry(&retryCount, err) {
				return false, nil
			}
			continue
		}
		defer resp.Body.Close()

		apiCallCount++

		zippedData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			if !a.retry(&retryCount, err) {
				return false, nil
			}
			continue
		}

		totalBytesCompressed += len(zippedData)

		reader, err := gzip.NewReader(bytes.NewReader(zippedData))
		if err != nil {
			if !a.retry(&retryCount, err) {
				return false, nil
			}
			continue
		}
		defer reader.Close()

		resultData, err := ioutil.ReadAll(reader)
		if err != nil {
			if !a.retry(&retryCount, err) {
				return false, nil
			}
			continue
		}

		totalBytesUncompressed += len(resultData)

		if strings.HasPrefix(string(resultData), `{"errors":[{"message":"rate limit for operation:`) {
			a.conf.ClientOptions.DebugLog("RATE LIMIT sleeping 5 seconds then retrying")
			time.Sleep(5 * time.Second)
			continue
		}

		var result map[string]interface{}
		if err := json.Unmarshal(resultData, &result); err != nil {
			if !a.retry(&retryCount, err) {
				return false, nil
			}
			continue
		}

		if _, ok := result["errors"]; ok {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("API error: %s", resultData))
			return false, result
		}

		return true, result
	}
}
