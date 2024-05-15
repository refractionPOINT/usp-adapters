package usp_cato

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"golang.org/x/net/context/ctxhttp"
)

const (
	defaultWriteTimeout = 60 * 10
)

var (
	apiCallCount           int
	totalBytesCompressed   int
	totalBytesUncompressed int
	start                  time.Time
	marker                 string
	configFile             string = "./config.txt"
)

type CatoConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	ApiKey          string                  `json:"apikey" yaml:"apikey"`
	AccountId       int                     `json:"accountid" yaml:"accountid"`
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

type CatoAdapter struct {
	conf         CatoConfig
	wg           sync.WaitGroup
	isRunning    uint32
	mRunning     sync.RWMutex
	uspClient    *uspclient.Client
	writeTimeout time.Duration

	chStopped chan struct{}
	wgSenders sync.WaitGroup

	ctx context.Context
}

func NewCatoAdapter(conf CatoConfig) (*CatoAdapter, chan struct{}, error) {
	a := &CatoAdapter{
		conf:      conf,
		isRunning: 1,
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	var err error
	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.chStopped = make(chan struct{})

	a.wgSenders.Add(1)
	if marker == "" {
		if _, err := os.Stat(configFile); err == nil {
			content, err := ioutil.ReadFile(configFile)
			if err != nil {
				fmt.Println(fmt.Sprintf("Error reading config file: %v", err))
			} else {
				marker = strings.TrimSpace(string(content))
			}
		}
	}
	
	go a.handleEvent(marker, strconv.Itoa(a.conf.AccountId), a.conf.ApiKey)

	go func() {
		a.wgSenders.Wait()
		close(a.chStopped)
	}()

	return a, a.chStopped, nil
}

func (a *CatoAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")

	a.mRunning.Lock()
	a.isRunning = 0
	a.mRunning.Unlock()

	a.wg.Done()
	a.wg.Wait()

	_, err := a.uspClient.Close()
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
		fmt.Println(err)
		return nil
	}

	return mapRepresentation
}

func (a *CatoAdapter) handleEvent(marker string, account_id string, api_key string) uintptr {

	start = time.Now()

	eventFilterString := ""
	eventSubfilterString := ""

	iteration := 1
	totalCount := 0

	for {
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
}`, account_id, marker, eventFilterString, eventSubfilterString)

		success, resp := a.send(query, account_id, api_key)
		if !success {
			fmt.Println(resp)
			os.Exit(1)
		}

		marker = resp["data"].(map[string]interface{})["eventsFeed"].(map[string]interface{})["marker"].(string)
		fetchedCount := int(resp["data"].(map[string]interface{})["eventsFeed"].(map[string]interface{})["fetchedCount"].(float64))
		totalCount += fetchedCount

		line := fmt.Sprintf("iteration:%d fetched:%d total_count:%d marker:%s", iteration, fetchedCount, totalCount, marker)

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
				a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			}
		}

		if err := ioutil.WriteFile(configFile, []byte(marker), 0644); err != nil {
			fmt.Println(fmt.Sprintf("Error writing config file: %v", err))
		}

		iteration++
		elapsed := time.Since(start)
		if elapsed.Hours() > 24 {
			break
		}
	}

	return 0
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
		if retryCount > 10 {
			log.Fatal("FATAL ERROR: retry count exceeded")
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			fmt.Println(fmt.Sprintf("ERROR %d: %v", retryCount, err))
			time.Sleep(2 * time.Second)
			retryCount++
			continue
		}

		req, err := http.NewRequest("POST", "https://api.catonetworks.com/api/v1/graphql2", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Println(fmt.Sprintf("ERROR %d: %v", retryCount, err))
			time.Sleep(2 * time.Second)
			retryCount++
			continue
		}

		for key, value := range headers {
			req.Header.Set(key, value)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resp, err := ctxhttp.Do(ctx, http.DefaultClient, req)
		if err != nil {
			fmt.Println(fmt.Sprintf("ERROR %d: %v, sleeping 2 seconds then retrying", retryCount, err))
			time.Sleep(2 * time.Second)
			retryCount++
			continue
		}
		defer resp.Body.Close()

		apiCallCount++

		zippedData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println(fmt.Sprintf("ERROR %d: %v", retryCount, err))
			time.Sleep(2 * time.Second)
			retryCount++
			continue
		}

		totalBytesCompressed += len(zippedData)

		reader, err := gzip.NewReader(bytes.NewReader(zippedData))
		if err != nil {
			fmt.Println(fmt.Sprintf("ERROR %d: %v", retryCount, err))
			time.Sleep(2 * time.Second)
			retryCount++
			continue
		}
		defer reader.Close()

		resultData, err := ioutil.ReadAll(reader)
		if err != nil {
			fmt.Println(fmt.Sprintf("ERROR %d: %v", retryCount, err))
			time.Sleep(2 * time.Second)
			retryCount++
			continue
		}

		totalBytesUncompressed += len(resultData)

		if strings.HasPrefix(string(resultData), `{"errors":[{"message":"rate limit for operation:`) {
			fmt.Println("RATE LIMIT sleeping 5 seconds then retrying")
			time.Sleep(5 * time.Second)
			continue
		}

		var result map[string]interface{}
		if err := json.Unmarshal(resultData, &result); err != nil {
			fmt.Println(fmt.Sprintf("ERROR %d: %v", retryCount, err))
			time.Sleep(2 * time.Second)
			retryCount++
			continue
		}

		if _, ok := result["errors"]; ok {
			fmt.Println(fmt.Sprintf("API error: %s", resultData))
			return false, result
		}

		return true, result
	}
}
