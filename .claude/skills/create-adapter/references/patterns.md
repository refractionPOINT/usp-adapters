# Annotated adapter patterns

Annotated, copy-ready snippets distilled from `threatlocker/`. Read the real
files (`threatlocker/api.go`, `threatlocker/client.go`,
`threatlocker/mock_test.go`) alongside this — they are the source of truth; this
is a map. Replace `MyAdapter`/`myadapter` with your names.

## Package + imports

```go
// Package usp_myadapter implements a USP adapter for the MyVendor API.
package usp_myadapter

import (
    "context"
    "sync"
    "time"

    "github.com/refractionPOINT/go-uspclient"
    "github.com/refractionPOINT/go-uspclient/protocol"
    "github.com/refractionPOINT/usp-adapters/utils"
)
```

## api.go — transport

```go
// HTTPError carries the status code so callers can classify retry vs. give-up
// without parsing error strings.
type HTTPError struct {
    StatusCode int
    URL        string
    Body       string
}

func (e *HTTPError) Error() string { /* include code, url, truncated body */ }

// isTransientError: retry 5xx, 429, and pre-response network errors; do NOT
// retry other 4xx (bad request/auth/not-found) or context cancellation.
func isTransientError(err error) bool { /* see threatlocker/api.go */ }

type MyAdapterClient struct {
    baseURL    string
    apiKey     string
    httpClient *http.Client
}

func NewMyAdapterClient(baseURL, apiKey string) *MyAdapterClient {
    return &MyAdapterClient{
        baseURL: strings.TrimRight(baseURL, "/"),
        apiKey:  apiKey,
        httpClient: &http.Client{
            Timeout:   60 * time.Second,
            Transport: &http.Transport{Dial: (&net.Dialer{Timeout: 10 * time.Second}).Dial},
        },
    }
}

// One generic request helper. Sets the auth header(s) the real API requires,
// returns the raw body, and turns non-2xx into *HTTPError.
func (c *MyAdapterClient) Get(ctx context.Context, path string, query url.Values) ([]byte, error) {
    // req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
    // req.Header.Set("Authorization", "Bearer "+c.apiKey)   // <-- match real API exactly
    // resp, err := c.httpClient.Do(req) ...
    // if resp.StatusCode != 200 { return nil, &HTTPError{...} }
}

func (c *MyAdapterClient) Close() { c.httpClient.CloseIdleConnections() }
```

## client.go — config + Validate

```go
type MyAdapterConfig struct {
    ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

    APIKey  string `json:"api_key" yaml:"api_key"`
    BaseURL string `json:"base_url" yaml:"base_url"`

    PageSize     int           `json:"page_size" yaml:"page_size"`
    PollInterval time.Duration `json:"poll_interval" yaml:"poll_interval"`
    DedupeTTL    time.Duration `json:"dedupe_ttl" yaml:"dedupe_ttl"`

    // Deduper is a test/embedder seam; not settable from config files.
    Deduper utils.Deduper `json:"-" yaml:"-"`
}

func (c *MyAdapterConfig) Validate() error {
    if err := c.ClientOptions.Validate(); err != nil {
        return fmt.Errorf("client_options: %v", err)
    }
    if c.APIKey == "" {
        return errors.New("missing api_key")
    }
    if c.PageSize <= 0 { c.PageSize = defaultPageSize }
    if c.PollInterval <= 0 { c.PollInterval = defaultPollInterval }
    if c.DedupeTTL <= 0 { c.DedupeTTL = defaultDedupeTTL }
    return nil
}
```

## client.go — the sink seam (makes E2E tests possible)

```go
// uspSink is the subset of *uspclient.Client the adapter uses; *uspclient.Client
// satisfies it unchanged, and tests inject an in-memory capture sink.
type uspSink interface {
    Ship(message *protocol.DataMessage, timeout time.Duration) error
    Drain(timeout time.Duration) error
    Close() ([]*protocol.DataMessage, error)
}

func NewMyAdapter(ctx context.Context, conf MyAdapterConfig) (*MyAdapter, chan struct{}, error) {
    return newMyAdapter(ctx, conf, nil) // nil => build a real uspclient.NewClient
}

func newMyAdapter(ctx context.Context, conf MyAdapterConfig, sink uspSink) (*MyAdapter, chan struct{}, error) {
    if err := conf.Validate(); err != nil { return nil, nil, err }

    a := &MyAdapter{conf: conf, ctx: ctx, doStop: utils.NewEvent()}

    a.deduper = conf.Deduper
    if a.deduper == nil {
        d, err := utils.NewLocalDeduper(dedupeBucketWindow, conf.DedupeTTL)
        if err != nil { return nil, nil, err }
        a.deduper = d
        a.ownsDeduper = true
    }

    if sink != nil {
        a.uspClient = sink
    } else {
        c, err := uspclient.NewClient(ctx, conf.ClientOptions)
        if err != nil { /* close owned deduper */ return nil, nil, err }
        a.uspClient = c
    }

    a.client = NewMyAdapterClient(conf.BaseURL, conf.APIKey)
    a.chStopped = make(chan struct{})
    a.wgSenders.Add(1)
    go a.run()
    go func() { a.wgSenders.Wait(); close(a.chStopped) }()
    return a, a.chStopped, nil
}
```

## client.go — lifecycle, poll loop, dedup, ship

```go
type MyAdapter struct {
    conf        MyAdapterConfig
    uspClient   uspSink
    client      *MyAdapterClient
    deduper     utils.Deduper
    ownsDeduper bool

    chStopped chan struct{}
    wgSenders sync.WaitGroup
    doStop    *utils.Event
    closeOnce sync.Once
    closeErr  error
    ctx       context.Context
}

// Close is idempotent (sync.Once): stop, drain, close, release deduper.
func (a *MyAdapter) Close() error {
    a.closeOnce.Do(func() {
        a.doStop.Set()
        a.wgSenders.Wait()
        err1 := a.uspClient.Drain(1 * time.Minute)
        _, err2 := a.uspClient.Close()
        a.client.Close()
        if a.ownsDeduper { a.deduper.Close() }
        if err1 != nil { a.closeErr = err1 } else { a.closeErr = err2 }
    })
    return a.closeErr
}

// run polls forever until asked to stop; first poll fires immediately.
func (a *MyAdapter) run() {
    defer a.wgSenders.Done()
    first := true
    for first || !a.doStop.WaitFor(a.conf.PollInterval) {
        first = false
        a.poll()
    }
}

// poll walks ALL pages every time. Dedup — not an early exit — ships each
// record once; the API paginates over a live list, so an early stop on
// already-seen records could skip a record that shifted across a page boundary.
func (a *MyAdapter) poll() {
    for page := 1; !a.doStop.IsSet(); page++ {
        items, ok := a.fetchPage(page) // retries transient errors; stops adapter on 401/403
        if !ok { return }
        if len(items) == 0 { break }
        for _, item := range items {
            if a.deduper.CheckAndAdd(a.dedupeKey(item)) { continue }
            if !a.ship(item) { return }
        }
        if len(items) < a.conf.PageSize { break } // short page == end
    }
}

// ship: verbatim payload, event type, event time. Handle backpressure.
func (a *MyAdapter) ship(item utils.Dict) bool {
    msg := &protocol.DataMessage{
        JsonPayload: item,
        EventType:   "myadapter_event",
        TimestampMs: a.eventTime(item),
    }
    if err := a.uspClient.Ship(msg, shipTimeout); err != nil {
        if err == uspclient.ErrorBufferFull {
            a.conf.ClientOptions.OnWarning("myadapter: stream falling behind")
            err = a.uspClient.Ship(msg, 1*time.Hour)
        }
        if err != nil {
            a.conf.ClientOptions.OnError(fmt.Errorf("myadapter: Ship(): %v", err))
            a.doStop.Set()
            return false
        }
    }
    return true
}
```

## Field extraction helpers (from utils.Dict)

- `item.FindOneString("path")` — first string at a `/`-separated path.
- `item.FindInt("path")` — all int matches (use to detect a present `0`).
- `utils.UnmarshalCleanJSON(string)` — decode a record preserving integer
  precision (don't use a plain `json.Unmarshal` into `map[string]interface{}`,
  which coerces ints to float64).

## Things to get right

- **Verbatim payloads.** Ship the vendor JSON unchanged; LimaCharlie does
  parsing/field-mapping in the cloud. Don't rename or restructure fields.
- **Auth failure stops the adapter** (401/403). Transient errors retry; other
  permanent errors abandon just the current poll.
- **Timestamps**: parse the vendor's format(s) into UnixMilli; fall back to
  `time.Now()` when absent/unparseable (and DebugLog it).
- **Pagination shape varies.** threatlocker uses page-number + short-page-end.
  If your API uses a cursor/`next` token or a `has_more` flag (see `1password/`),
  loop on that instead — and make the mock use the same mechanism.
