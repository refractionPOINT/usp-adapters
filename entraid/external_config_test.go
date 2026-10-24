package usp_entraid_test

import (
	"testing"
	"time"

	uspclient "github.com/refractionPOINT/go-uspclient"
	entraid "github.com/refractionPOINT/usp-adapters/entraid"
)

func TestExternalEntraConfigLiteralCompatibility(t *testing.T) {
	cfg := entraid.EntraIDConfig{uspclient.ClientOptions{}, "fixture-tenant", "fixture-client", "fixture-secret", "risk_detections", "enterprise", "", "", time.Second}
	if cfg.TenantID != "fixture-tenant" {
		t.Fatal("configuration fields changed")
	}
}
