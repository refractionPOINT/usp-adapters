package usp_halopsa

import (
	"errors"
	"net/url"
	"reflect"
	"testing"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// validClientOptions returns a minimal ClientOptions that passes
// uspclient.ClientOptions.Validate(), so tests can focus on the
// HaloPSA-specific validation.
func validClientOptions() uspclient.ClientOptions {
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "11111111-1111-1111-1111-111111111111",
			InstallationKey: "22222222-2222-2222-2222-222222222222",
		},
		Platform: "json",
	}
}

func TestTokenURL(t *testing.T) {
	cases := []struct {
		name string
		conf HaloPSAConfig
		want string
	}{
		{
			name: "instance only",
			conf: HaloPSAConfig{InstanceURL: "https://acme.halopsa.com"},
			want: "https://acme.halopsa.com/auth/token",
		},
		{
			name: "trailing slash is trimmed",
			conf: HaloPSAConfig{InstanceURL: "https://acme.halopsa.com/"},
			want: "https://acme.halopsa.com/auth/token",
		},
		{
			name: "auth_url override",
			conf: HaloPSAConfig{InstanceURL: "https://acme.halopsa.com", AuthURL: "https://auth.example.com/auth/"},
			want: "https://auth.example.com/auth/token",
		},
		{
			name: "hosted tenant parameter",
			conf: HaloPSAConfig{InstanceURL: "https://acme.halopsa.com", Tenant: "acme corp"},
			want: "https://acme.halopsa.com/auth/token?tenant=acme+corp",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.conf.tokenURL(); got != c.want {
				t.Fatalf("tokenURL() = %q, want %q", got, c.want)
			}
		})
	}
}

func TestResourceBaseURL(t *testing.T) {
	cases := []struct {
		name string
		conf HaloPSAConfig
		want string
	}{
		{
			name: "derived from instance",
			conf: HaloPSAConfig{InstanceURL: "https://acme.halopsa.com/"},
			want: "https://acme.halopsa.com/api",
		},
		{
			name: "api_url override",
			conf: HaloPSAConfig{InstanceURL: "https://acme.halopsa.com", APIURL: "https://api.example.com/api/"},
			want: "https://api.example.com/api",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.conf.resourceBaseURL(); got != c.want {
				t.Fatalf("resourceBaseURL() = %q, want %q", got, c.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	base := HaloPSAConfig{
		ClientOptions: validClientOptions(),
		InstanceURL:   "https://acme.halopsa.com",
		ClientID:      "id",
		ClientSecret:  "secret",
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}

	noURL := base
	noURL.InstanceURL = ""
	if err := noURL.Validate(); err == nil {
		t.Fatal("expected error when no instance/auth/api URL is set")
	}

	splitURLs := HaloPSAConfig{
		ClientOptions: validClientOptions(),
		AuthURL:       "https://auth.example.com/auth",
		APIURL:        "https://api.example.com/api",
		ClientID:      "id",
		ClientSecret:  "secret",
	}
	if err := splitURLs.Validate(); err != nil {
		t.Fatalf("expected split auth/api URLs to be valid, got %v", err)
	}

	noSecret := base
	noSecret.ClientSecret = ""
	if err := noSecret.Validate(); err == nil {
		t.Fatal("expected error when client_secret is missing")
	}
}

func TestParseListResponse(t *testing.T) {
	t.Run("envelope with auto-detected array", func(t *testing.T) {
		body := []byte(`{"record_count":3,"audit":[{"id":7},{"id":6},{"id":5}]}`)
		items, count, err := parseListResponse(body, "")
		if err != nil {
			t.Fatal(err)
		}
		if count != 3 {
			t.Fatalf("record_count = %d, want 3", count)
		}
		if got := idsOf(items); !reflect.DeepEqual(got, []uint64{7, 6, 5}) {
			t.Fatalf("ids = %v, want [7 6 5]", got)
		}
	})

	t.Run("envelope with explicit data_field", func(t *testing.T) {
		body := []byte(`{"record_count":1,"tickets":[{"id":42}]}`)
		items, _, err := parseListResponse(body, "tickets")
		if err != nil {
			t.Fatal(err)
		}
		if got := idsOf(items); !reflect.DeepEqual(got, []uint64{42}) {
			t.Fatalf("ids = %v, want [42]", got)
		}
	})

	t.Run("bare json array", func(t *testing.T) {
		body := []byte(`[{"id":1},{"id":2}]`)
		items, count, err := parseListResponse(body, "")
		if err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("count = %d, want 2", count)
		}
		if got := idsOf(items); !reflect.DeepEqual(got, []uint64{1, 2}) {
			t.Fatalf("ids = %v, want [1 2]", got)
		}
	})

	t.Run("empty body", func(t *testing.T) {
		items, count, err := parseListResponse([]byte("  "), "")
		if err != nil || items != nil || count != 0 {
			t.Fatalf("expected empty result, got items=%v count=%d err=%v", items, count, err)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		if _, _, err := parseListResponse([]byte(`{not json`), ""); err == nil {
			t.Fatal("expected error for invalid json")
		}
	})
}

// fakePager turns a list of newest-first pages into a fetchPage function.
func fakePager(pages [][]utils.Dict) func(int) ([]utils.Dict, int, error) {
	total := 0
	for _, p := range pages {
		total += len(p)
	}
	return func(pageNo int) ([]utils.Dict, int, error) {
		if pageNo < 1 || pageNo > len(pages) {
			return nil, total, nil
		}
		return pages[pageNo-1], total, nil
	}
}

func TestCollectNewItems(t *testing.T) {
	t.Run("steady state single page", func(t *testing.T) {
		pages := [][]utils.Dict{mkItems(103, 102, 101, 100, 99)}
		items, newMax, err := collectNewItems(fakePager(pages), "id", 100, nil)
		if err != nil {
			t.Fatal(err)
		}
		if newMax != 103 {
			t.Fatalf("newMax = %d, want 103", newMax)
		}
		if got := idsOf(items); !reflect.DeepEqual(got, []uint64{101, 102, 103}) {
			t.Fatalf("ids = %v, want [101 102 103] (oldest-first)", got)
		}
	})

	t.Run("no new events", func(t *testing.T) {
		pages := [][]utils.Dict{mkItems(100, 99, 98)}
		items, newMax, err := collectNewItems(fakePager(pages), "id", 100, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != 0 || newMax != 100 {
			t.Fatalf("expected no items and cursor 100, got items=%d newMax=%d", len(items), newMax)
		}
	})

	t.Run("multi-page catch up", func(t *testing.T) {
		pages := [][]utils.Dict{
			mkItems(110, 109, 108, 107, 106),
			mkItems(105, 104, 103, 102, 101),
			mkItems(100, 99, 98, 97, 96),
		}
		items, newMax, err := collectNewItems(fakePager(pages), "id", 100, nil)
		if err != nil {
			t.Fatal(err)
		}
		if newMax != 110 {
			t.Fatalf("newMax = %d, want 110", newMax)
		}
		want := []uint64{101, 102, 103, 104, 105, 106, 107, 108, 109, 110}
		if got := idsOf(items); !reflect.DeepEqual(got, want) {
			t.Fatalf("ids = %v, want %v", got, want)
		}
	})

	t.Run("deduplicates records that shift between pages", func(t *testing.T) {
		// 106 appears on both page 1 and page 2 because new records were
		// inserted while paginating.
		pages := [][]utils.Dict{
			mkItems(110, 109, 108, 107, 106),
			mkItems(106, 105, 104, 103, 102),
			mkItems(101, 100, 99),
		}
		items, newMax, err := collectNewItems(fakePager(pages), "id", 100, nil)
		if err != nil {
			t.Fatal(err)
		}
		if newMax != 110 {
			t.Fatalf("newMax = %d, want 110", newMax)
		}
		want := []uint64{101, 102, 103, 104, 105, 106, 107, 108, 109, 110}
		if got := idsOf(items); !reflect.DeepEqual(got, want) {
			t.Fatalf("ids = %v, want %v (each id exactly once)", got, want)
		}
	})

	t.Run("empty endpoint", func(t *testing.T) {
		items, newMax, err := collectNewItems(fakePager(nil), "id", 50, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != 0 || newMax != 50 {
			t.Fatalf("expected no items and cursor unchanged, got items=%d newMax=%d", len(items), newMax)
		}
	})

	t.Run("fetch error is propagated and cursor preserved", func(t *testing.T) {
		boom := errors.New("boom")
		fp := func(int) ([]utils.Dict, int, error) { return nil, 0, boom }
		_, newMax, err := collectNewItems(fp, "id", 77, nil)
		if !errors.Is(err, boom) {
			t.Fatalf("expected boom error, got %v", err)
		}
		if newMax != 77 {
			t.Fatalf("cursor = %d, want preserved 77", newMax)
		}
	})

	t.Run("stop function halts pagination", func(t *testing.T) {
		// Every page is full of new records, so without the stop function
		// this would page forever.
		calls := 0
		fp := func(int) ([]utils.Dict, int, error) {
			calls++
			return mkItems(1000, 999, 998, 997, 996), 1 << 30, nil
		}
		stopped := false
		stop := func() bool {
			if calls >= 3 {
				stopped = true
			}
			return stopped
		}
		if _, _, err := collectNewItems(fp, "id", 0, stop); err != nil {
			t.Fatal(err)
		}
		if calls > 3 {
			t.Fatalf("expected pagination to stop near 3 pages, made %d calls", calls)
		}
	})
}

func TestApplyExtraParams(t *testing.T) {
	// The CLI/env parser auto-types values, so an extra_params map can
	// receive bool/int/float as well as strings. Each must be stringified
	// so url.Values can carry it.
	q := url.Values{}
	applyExtraParams(q, map[string]interface{}{
		"excludesys": true,
		"user_id":    42,
		"ratio":      1.5,
		"name":       "alice",
		"missing":    nil,
	})
	cases := map[string]string{
		"excludesys": "true",
		"user_id":    "42",
		"ratio":      "1.5",
		"name":       "alice",
		"missing":    "",
	}
	for k, want := range cases {
		if got := q.Get(k); got != want {
			t.Fatalf("q.Get(%q) = %q, want %q", k, got, want)
		}
	}
}

func TestMaxID(t *testing.T) {
	if got := maxID(mkItems(3, 9, 5), "id"); got != 9 {
		t.Fatalf("maxID = %d, want 9", got)
	}
	if got := maxID(nil, "id"); got != 0 {
		t.Fatalf("maxID of empty = %d, want 0", got)
	}
}

func TestTruncate(t *testing.T) {
	long := make([]byte, maxErrBodyLen+100)
	for i := range long {
		long[i] = 'x'
	}
	out := truncate(long)
	if len(out) <= maxErrBodyLen || out[len(out)-len("(truncated)"):] != "(truncated)" {
		t.Fatalf("expected truncated marker, got len %d", len(out))
	}
	if truncate([]byte("  short  ")) != "short" {
		t.Fatal("expected short body to be trimmed and returned as-is")
	}
}

// mkItems builds newest-first records carrying the given ids.
func mkItems(ids ...int) []utils.Dict {
	out := make([]utils.Dict, 0, len(ids))
	for _, id := range ids {
		out = append(out, utils.Dict{"id": uint64(id)})
	}
	return out
}

func idsOf(items []utils.Dict) []uint64 {
	out := make([]uint64, 0, len(items))
	for _, it := range items {
		id, _ := it.GetInt("id")
		out = append(out, id)
	}
	return out
}
