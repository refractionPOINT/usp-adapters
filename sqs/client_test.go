package usp_sqs

import (
	"strings"
	"testing"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

func testClientOptions(t *testing.T) uspclient.ClientOptions {
	t.Helper()
	return uspclient.ClientOptions{
		Identity: uspclient.Identity{
			Oid:             "00000000-0000-0000-0000-000000000000",
			InstallationKey: "00000000-0000-0000-0000-000000000000",
		},
		Platform: "aws",
	}
}

func TestSQSConfigValidate(t *testing.T) {
	rolesAnywhere := utils.AWSRolesAnywhereConfig{
		Certificate:    "cert",
		PrivateKey:     "key",
		TrustAnchorARN: "arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/abc",
		ProfileARN:     "arn:aws:rolesanywhere:us-east-1:123456789012:profile/abc",
		RoleARN:        "arn:aws:iam::123456789012:role/abc",
	}
	for _, c := range []struct {
		name    string
		conf    SQSConfig
		errPart string
	}{
		{
			name: "static keys",
			conf: SQSConfig{ClientOptions: testClientOptions(t), AccessKey: "ak", SecretKey: "sk", Region: "us-east-1", QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/q"},
		},
		{
			name: "roles anywhere",
			conf: SQSConfig{ClientOptions: testClientOptions(t), RolesAnywhere: rolesAnywhere, Region: "us-east-1", QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/q"},
		},
		{
			name:    "no auth",
			conf:    SQSConfig{ClientOptions: testClientOptions(t), Region: "us-east-1", QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/q"},
			errPart: "missing access_key",
		},
		{
			name:    "both auth methods",
			conf:    SQSConfig{ClientOptions: testClientOptions(t), AccessKey: "ak", SecretKey: "sk", RolesAnywhere: rolesAnywhere, Region: "us-east-1", QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/q"},
			errPart: "not both",
		},
		{
			name:    "missing region",
			conf:    SQSConfig{ClientOptions: testClientOptions(t), AccessKey: "ak", SecretKey: "sk", QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/q"},
			errPart: "missing region",
		},
	} {
		err := c.conf.Validate()
		if c.errPart == "" {
			if err != nil {
				t.Errorf("%s: unexpected error: %v", c.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), c.errPart) {
			t.Errorf("%s: expected error containing %q, got: %v", c.name, c.errPart, err)
		}
	}
}
