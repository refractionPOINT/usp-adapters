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
	Name      string   `yaml:"name"`
	SecretEnv []string `yaml:"secretEnv"`
	Env       []string `yaml:"env"`
	Args      []string `yaml:"args"`
	WaitFor   []string `yaml:"waitFor"`
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

// The steps a step waits on, by id. Without waitFor, Cloud Build waits on
// every earlier step; "-" means none.
func stepDeps(steps []releaseStep, i int) []string {
	if steps[i].WaitFor == nil {
		var ids []string
		for _, s := range steps[:i] {
			ids = append(ids, s.ID)
		}
		return ids
	}
	var ids []string
	for _, d := range steps[i].WaitFor {
		if d != "-" {
			ids = append(ids, d)
		}
	}
	return ids
}

// Every step that finishes before step i can start, directly or through
// other steps.
func stepsBefore(steps []releaseStep, i int) map[string]bool {
	index := map[string]int{}
	for j, s := range steps {
		index[s.ID] = j
	}
	before := map[string]bool{}
	var visit func(int)
	visit = func(j int) {
		for _, d := range stepDeps(steps, j) {
			k, ok := index[d]
			if !ok || before[d] {
				continue
			}
			before[d] = true
			visit(k)
		}
	}
	visit(i)
	return before
}

// Cloud Build only rejects a waitFor that names an unknown step when the
// build is submitted, which for this file is the release itself.
func TestReleaseBuildWaitForNamesKnownSteps(t *testing.T) {
	b := loadReleaseBuild(t)

	ids := map[string]bool{}
	for _, s := range b.Steps {
		if s.ID == "" {
			t.Errorf("step %q has no id", s.Name)
		}
		ids[s.ID] = true
	}
	for i, s := range b.Steps {
		for _, d := range stepDeps(b.Steps, i) {
			if !ids[d] {
				t.Errorf("step %q waits on unknown step %q", s.ID, d)
			}
		}
	}
}

// Nothing may be published until every step that can fail the release (all
// builds and both signing steps) has finished, so a late failure cannot leave
// a partial release. v1.32.53 had pushed its Docker images and connectivity
// binaries when signing failed.
func TestReleaseBuildPublishesOnlyAfterEveryBuildAndSigningStep(t *testing.T) {
	b := loadReleaseBuild(t)

	var mustFinish []string
	for _, s := range b.Steps {
		if strings.HasPrefix(s.ID, "build-") || strings.HasPrefix(s.ID, "sign-") {
			mustFinish = append(mustFinish, s.ID)
		}
	}
	published := 0
	for i, s := range b.Steps {
		if !strings.HasPrefix(s.ID, "copy-") {
			continue
		}
		published++
		before := stepsBefore(b.Steps, i)
		for _, id := range mustFinish {
			if !before[id] {
				t.Errorf("publish step %q can start before %q has finished", s.ID, id)
			}
		}
	}
	if published == 0 {
		t.Fatal("no copy-* publish steps found")
	}
}
