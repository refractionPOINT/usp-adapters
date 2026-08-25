package usp_s3

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

func TestS3ConfigValidate(t *testing.T) {
	rolesAnywhere := utils.AWSRolesAnywhereConfig{
		Certificate:    "cert",
		PrivateKey:     "key",
		TrustAnchorARN: "arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/abc",
		ProfileARN:     "arn:aws:rolesanywhere:us-east-1:123456789012:profile/abc",
		RoleARN:        "arn:aws:iam::123456789012:role/abc",
	}
	for _, c := range []struct {
		name    string
		conf    S3Config
		errPart string
	}{
		{
			name: "static keys",
			conf: S3Config{ClientOptions: testClientOptions(t), BucketName: "b", AccessKey: "ak", SecretKey: "sk"},
		},
		{
			name: "roles anywhere",
			conf: S3Config{ClientOptions: testClientOptions(t), BucketName: "b", RolesAnywhere: rolesAnywhere},
		},
		{
			name:    "missing bucket",
			conf:    S3Config{ClientOptions: testClientOptions(t), AccessKey: "ak", SecretKey: "sk"},
			errPart: "missing bucket_name",
		},
		{
			name:    "no auth",
			conf:    S3Config{ClientOptions: testClientOptions(t), BucketName: "b"},
			errPart: "missing access_key",
		},
		{
			name:    "both auth methods",
			conf:    S3Config{ClientOptions: testClientOptions(t), BucketName: "b", AccessKey: "ak", SecretKey: "sk", RolesAnywhere: rolesAnywhere},
			errPart: "not both",
		},
		{
			name:    "partial roles anywhere",
			conf:    S3Config{ClientOptions: testClientOptions(t), BucketName: "b", RolesAnywhere: utils.AWSRolesAnywhereConfig{Certificate: "cert"}},
			errPart: "missing roles_anywhere.private_key",
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
