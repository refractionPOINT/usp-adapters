package buildscripts

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The release build signs the Windows and macOS adapters with the build's own
// service account. These checks keep a stored signing key from coming back
// and keep the build on its own account, which only it can sign with.

type releaseStep struct {
	ID        string   `yaml:"id"`
	SecretEnv []string `yaml:"secretEnv"`
	Env       []string `yaml:"env"`
	Args      []string `yaml:"args"`
}

type releaseBuild struct {
	Steps          []releaseStep `yaml:"steps"`
	ServiceAccount string        `yaml:"serviceAccount"`
	LogsBucket     string        `yaml:"logsBucket"`
	Options        struct {
		Logging string `yaml:"logging"`
	} `yaml:"options"`
	AvailableSecrets struct {
		SecretManager []struct {
			VersionName string `yaml:"versionName"`
			Env         string `yaml:"env"`
		} `yaml:"secretManager"`
	} `yaml:"availableSecrets"`
}

func loadReleaseBuild(t *testing.T) releaseBuild {
	t.Helper()
	data, err := os.ReadFile("../cloudbuild_release.yaml")
	if err != nil {
		t.Fatalf("reading cloudbuild_release.yaml: %v", err)
	}
	var b releaseBuild
	if err := yaml.Unmarshal(data, &b); err != nil {
		t.Fatalf("parsing cloudbuild_release.yaml: %v", err)
	}
	return b
}

func TestReleaseBuildSignsWithoutStoredKey(t *testing.T) {
	b := loadReleaseBuild(t)

	signSteps := map[string]bool{"sign-binaries": false, "sign-win64-legacy": false}
	for _, s := range b.Steps {
		if _, ok := signSteps[s.ID]; ok {
			signSteps[s.ID] = true
		}
		for _, e := range append(append([]string{}, s.SecretEnv...), s.Env...) {
			if strings.HasPrefix(e, "CODE_SIGNING_KEY") {
				t.Errorf("step %q is given CODE_SIGNING_KEY: signing must use the build's own account", s.ID)
			}
		}
		if strings.Contains(strings.Join(s.Args, " "), "--base64-key") {
			t.Errorf("step %q passes a signing key on the command line", s.ID)
		}
	}
	for id, found := range signSteps {
		if !found {
			t.Errorf("signing step %q not found", id)
		}
	}
	for _, sec := range b.AvailableSecrets.SecretManager {
		if sec.Env == "CODE_SIGNING_KEY" || strings.Contains(sec.VersionName, "/secrets/CODE_SIGNING_KEY/") {
			t.Errorf("availableSecrets still loads the signing key (%s)", sec.VersionName)
		}
	}
}

func TestReleaseBuildRunsAsItsOwnAccount(t *testing.T) {
	b := loadReleaseBuild(t)

	if !strings.Contains(b.ServiceAccount, "/serviceAccounts/usp-adapters-release@") {
		t.Errorf("serviceAccount = %q, want the dedicated usp-adapters-release account", b.ServiceAccount)
	}
	// Cloud Build refuses a user-specified service account whose logs would go
	// to the Google-owned default bucket.
	switch b.Options.Logging {
	case "CLOUD_LOGGING_ONLY":
	case "GCS_ONLY":
		if b.LogsBucket == "" {
			t.Errorf("options.logging is GCS_ONLY without a logsBucket")
		}
	default:
		t.Errorf("options.logging = %q, want CLOUD_LOGGING_ONLY (or GCS_ONLY with a logsBucket)", b.Options.Logging)
	}
}
