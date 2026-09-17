package utils

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	testTrustAnchorARN = "arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/6d4f4c6f-8b3a-4c22-9d1e-000000000000"
	testProfileARN     = "arn:aws:rolesanywhere:us-east-1:123456789012:profile/2f9c8a10-1111-2222-3333-000000000000"
	testRoleARN        = "arn:aws:iam::123456789012:role/lc-adapter"
)

type testKeyPair struct {
	certPEM string
	keyPEM  string
	cert    *x509.Certificate
}

func makeTestCert(t *testing.T, keyType string, serial int64, issuerKey crypto.Signer, issuerCert *x509.Certificate) (testKeyPair, crypto.Signer) {
	t.Helper()
	var key crypto.Signer
	var err error
	switch keyType {
	case "rsa":
		key, err = rsa.GenerateKey(rand.Reader, 2048)
	case "ec":
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	default:
		t.Fatalf("unknown key type %s", keyType)
	}
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: fmt.Sprintf("test-%s-%d", keyType, serial)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	parent := template
	signKey := key
	if issuerCert != nil {
		parent = issuerCert
		signKey = issuerKey
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, key.Public(), signKey)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return testKeyPair{certPEM: string(certPEM), keyPEM: string(keyPEM), cert: cert}, key
}

// newFakeRolesAnywhereServer returns an httptest server that validates
// incoming CreateSession requests exactly like the real service: it
// rebuilds the canonical request from the headers listed in the
// Authorization header and verifies the signature against the public
// key of the certificate presented in X-Amz-X509.
func newFakeRolesAnywhereServer(t *testing.T, expiration time.Time, onRequest func(r *http.Request, body []byte)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(makeFakeRolesAnywhereHandler(t, expiration, onRequest)))
}

func makeFakeRolesAnywhereHandler(t *testing.T, expiration time.Time, onRequest func(r *http.Request, body []byte)) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sessions" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		auth := r.Header.Get("Authorization")
		algorithm, credential, signedHeaders, signature, err := parseTestAuthorization(auth)
		if err != nil {
			t.Errorf("authorization header %q: %v", auth, err)
			w.WriteHeader(http.StatusForbidden)
			return
		}

		certDER, err := base64.StdEncoding.DecodeString(r.Header.Get("X-Amz-X509"))
		if err != nil {
			t.Errorf("x-amz-x509 decode: %v", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		cert, err := x509.ParseCertificate(certDER)
		if err != nil {
			t.Errorf("x-amz-x509 parse: %v", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}

		serialAndScope := strings.SplitN(credential, "/", 2)
		if serialAndScope[0] != cert.SerialNumber.String() {
			t.Errorf("credential serial %q does not match certificate serial %q", serialAndScope[0], cert.SerialNumber.String())
		}

		// Rebuild the canonical request from what was actually received.
		canonicalHeaders := strings.Builder{}
		for _, h := range strings.Split(signedHeaders, ";") {
			v := r.Header.Get(h)
			if h == "host" {
				v = r.Host
			}
			canonicalHeaders.WriteString(h + ":" + v + "\n")
		}
		payloadHash := sha256.Sum256(body)
		canonicalRequest := strings.Join([]string{
			"POST",
			"/sessions",
			"",
			canonicalHeaders.String(),
			signedHeaders,
			hex.EncodeToString(payloadHash[:]),
		}, "\n")
		canonicalHash := sha256.Sum256([]byte(canonicalRequest))
		stringToSign := strings.Join([]string{
			algorithm,
			r.Header.Get("X-Amz-Date"),
			serialAndScope[1],
			hex.EncodeToString(canonicalHash[:]),
		}, "\n")
		digest := sha256.Sum256([]byte(stringToSign))

		sig, err := hex.DecodeString(signature)
		if err != nil {
			t.Errorf("signature decode: %v", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch pub := cert.PublicKey.(type) {
		case *rsa.PublicKey:
			if algorithm != "AWS4-X509-RSA-SHA256" {
				t.Errorf("algorithm %q does not match RSA key", algorithm)
			}
			if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
				t.Errorf("RSA signature verification failed: %v", err)
				w.WriteHeader(http.StatusForbidden)
				return
			}
		case *ecdsa.PublicKey:
			if algorithm != "AWS4-X509-ECDSA-SHA256" {
				t.Errorf("algorithm %q does not match ECDSA key", algorithm)
			}
			if !ecdsa.VerifyASN1(pub, digest[:], sig) {
				t.Errorf("ECDSA signature verification failed")
				w.WriteHeader(http.StatusForbidden)
				return
			}
		default:
			t.Errorf("unexpected public key type %T", pub)
			w.WriteHeader(http.StatusForbidden)
			return
		}

		if onRequest != nil {
			onRequest(r, body)
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"credentialSet": []map[string]interface{}{{
				"credentials": map[string]interface{}{
					"accessKeyId":     "ASIAEXAMPLE",
					"secretAccessKey": "secretExample",
					"sessionToken":    "tokenExample",
					"expiration":      expiration.UTC().Format(time.RFC3339),
				},
			}},
		})
	}
}

func parseTestAuthorization(auth string) (algorithm string, credential string, signedHeaders string, signature string, err error) {
	algoAndRest := strings.SplitN(auth, " ", 2)
	if len(algoAndRest) != 2 {
		return "", "", "", "", fmt.Errorf("malformed header")
	}
	algorithm = algoAndRest[0]
	for _, part := range strings.Split(algoAndRest[1], ", ") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return "", "", "", "", fmt.Errorf("malformed component %q", part)
		}
		switch kv[0] {
		case "Credential":
			credential = kv[1]
		case "SignedHeaders":
			signedHeaders = kv[1]
		case "Signature":
			signature = kv[1]
		}
	}
	if credential == "" || signedHeaders == "" || signature == "" {
		return "", "", "", "", fmt.Errorf("missing component")
	}
	return algorithm, credential, signedHeaders, signature, nil
}

func testRetrieve(t *testing.T, keyType string) {
	pair, _ := makeTestCert(t, keyType, 424242, nil, nil)

	var gotBody map[string]interface{}
	server := newFakeRolesAnywhereServer(t, time.Now().Add(time.Hour), func(r *http.Request, body []byte) {
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Errorf("request body: %v", err)
		}
	})
	defer server.Close()

	conf := AWSRolesAnywhereConfig{
		Certificate:    pair.certPEM,
		PrivateKey:     pair.keyPEM,
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	p, err := newRolesAnywhereProvider(conf)
	if err != nil {
		t.Fatalf("newRolesAnywhereProvider: %v", err)
	}
	p.endpoint = server.URL

	v, err := p.Retrieve()
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if v.AccessKeyID != "ASIAEXAMPLE" || v.SecretAccessKey != "secretExample" || v.SessionToken != "tokenExample" {
		t.Errorf("unexpected credentials: %+v", v)
	}
	if v.ProviderName != rolesAnywhereProviderName {
		t.Errorf("unexpected provider name: %s", v.ProviderName)
	}
	if p.IsExpired() {
		t.Errorf("credentials should not be expired right after retrieval")
	}
	for k, expected := range map[string]string{
		"profileArn":     testProfileARN,
		"roleArn":        testRoleARN,
		"trustAnchorArn": testTrustAnchorARN,
	} {
		if gotBody[k] != expected {
			t.Errorf("request body %s = %v, expected %s", k, gotBody[k], expected)
		}
	}
	if gotBody["durationSeconds"] != float64(rolesAnywhereSessionSeconds) {
		t.Errorf("request body durationSeconds = %v", gotBody["durationSeconds"])
	}
}

func TestRolesAnywhereRetrieveRSA(t *testing.T) {
	testRetrieve(t, "rsa")
}

func TestRolesAnywhereRetrieveECDSA(t *testing.T) {
	testRetrieve(t, "ec")
}

func TestRolesAnywhereRetryOnServerError(t *testing.T) {
	pair, _ := makeTestCert(t, "rsa", 21, nil, nil)

	failuresLeft := 1
	handler := makeFakeRolesAnywhereHandler(t, time.Now().Add(time.Hour), nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failuresLeft > 0 {
			failuresLeft--
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		handler(w, r)
	}))
	defer server.Close()

	conf := AWSRolesAnywhereConfig{
		Certificate:    pair.certPEM,
		PrivateKey:     pair.keyPEM,
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	p, err := newRolesAnywhereProvider(conf)
	if err != nil {
		t.Fatalf("newRolesAnywhereProvider: %v", err)
	}
	p.endpoint = server.URL

	v, err := p.Retrieve()
	if err != nil {
		t.Fatalf("Retrieve should have retried past the 503: %v", err)
	}
	if v.AccessKeyID != "ASIAEXAMPLE" {
		t.Errorf("unexpected credentials: %+v", v)
	}
}

func TestRolesAnywhereClockSkew(t *testing.T) {
	pair, _ := makeTestCert(t, "rsa", 23, nil, nil)

	// Simulate a local clock far ahead of AWS: the returned expiration
	// is already in the past. The provider must not enter a state where
	// every SDK call triggers a new CreateSession.
	server := newFakeRolesAnywhereServer(t, time.Now().Add(-2*time.Hour), nil)
	defer server.Close()

	conf := AWSRolesAnywhereConfig{
		Certificate:    pair.certPEM,
		PrivateKey:     pair.keyPEM,
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	p, err := newRolesAnywhereProvider(conf)
	if err != nil {
		t.Fatalf("newRolesAnywhereProvider: %v", err)
	}
	p.endpoint = server.URL

	if _, err := p.Retrieve(); err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if p.IsExpired() {
		t.Errorf("credentials should be kept for a minimum period despite a skewed expiration")
	}
}

func TestRolesAnywhereCertificateChain(t *testing.T) {
	caPair, caKey := makeTestCert(t, "rsa", 1, nil, nil)
	leafPair, _ := makeTestCert(t, "rsa", 2, caKey, caPair.cert)

	chainSeen := ""
	server := newFakeRolesAnywhereServer(t, time.Now().Add(time.Hour), func(r *http.Request, body []byte) {
		chainSeen = r.Header.Get("X-Amz-X509-Chain")
	})
	defer server.Close()

	conf := AWSRolesAnywhereConfig{
		Certificate:    leafPair.certPEM + caPair.certPEM,
		PrivateKey:     leafPair.keyPEM,
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	p, err := newRolesAnywhereProvider(conf)
	if err != nil {
		t.Fatalf("newRolesAnywhereProvider: %v", err)
	}
	p.endpoint = server.URL

	if _, err := p.Retrieve(); err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	expectedChain := base64.StdEncoding.EncodeToString(caPair.cert.Raw)
	if chainSeen != expectedChain {
		t.Errorf("x-amz-x509-chain = %q, expected %q", chainSeen, expectedChain)
	}
}

func TestRolesAnywhereCertificateChainCAFirst(t *testing.T) {
	caPair, caKey := makeTestCert(t, "rsa", 3, nil, nil)
	leafPair, _ := makeTestCert(t, "rsa", 4, caKey, caPair.cert)

	// The fake server asserts that the Credential serial matches the
	// certificate presented in X-Amz-X509, so this passing proves the
	// leaf was correctly identified despite the CA coming first.
	server := newFakeRolesAnywhereServer(t, time.Now().Add(time.Hour), nil)
	defer server.Close()

	conf := AWSRolesAnywhereConfig{
		Certificate:    caPair.certPEM + leafPair.certPEM,
		PrivateKey:     leafPair.keyPEM,
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	p, err := newRolesAnywhereProvider(conf)
	if err != nil {
		t.Fatalf("newRolesAnywhereProvider: %v", err)
	}
	p.endpoint = server.URL

	if _, err := p.Retrieve(); err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
}

func TestRolesAnywhereKeyCertificateMismatch(t *testing.T) {
	pairA, _ := makeTestCert(t, "rsa", 5, nil, nil)
	pairB, _ := makeTestCert(t, "rsa", 6, nil, nil)

	conf := AWSRolesAnywhereConfig{
		Certificate:    pairA.certPEM,
		PrivateKey:     pairB.keyPEM,
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	if _, err := newRolesAnywhereProvider(conf); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected key/certificate mismatch error, got: %v", err)
	}
}

func TestRolesAnywhereEscapedPEM(t *testing.T) {
	pair, _ := makeTestCert(t, "rsa", 7, nil, nil)

	server := newFakeRolesAnywhereServer(t, time.Now().Add(time.Hour), nil)
	defer server.Close()

	// Simulate a PEM pasted as a single line in a UI.
	conf := AWSRolesAnywhereConfig{
		Certificate:    strings.ReplaceAll(pair.certPEM, "\n", "\\n"),
		PrivateKey:     strings.ReplaceAll(pair.keyPEM, "\n", "\\n"),
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	p, err := newRolesAnywhereProvider(conf)
	if err != nil {
		t.Fatalf("newRolesAnywhereProvider: %v", err)
	}
	p.endpoint = server.URL
	if _, err := p.Retrieve(); err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
}

func TestRolesAnywhereErrorResponse(t *testing.T) {
	pair, _ := makeTestCert(t, "rsa", 9, nil, nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"message":"AccessDeniedException"}`)
	}))
	defer server.Close()

	conf := AWSRolesAnywhereConfig{
		Certificate:    pair.certPEM,
		PrivateKey:     pair.keyPEM,
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	p, err := newRolesAnywhereProvider(conf)
	if err != nil {
		t.Fatalf("newRolesAnywhereProvider: %v", err)
	}
	p.endpoint = server.URL
	if _, err := p.Retrieve(); err == nil || !strings.Contains(err.Error(), "status 403") {
		t.Fatalf("expected 403 error, got: %v", err)
	}
}

func TestRolesAnywhereEndpointFromARN(t *testing.T) {
	for _, c := range []struct {
		arn      string
		region   string
		endpoint string
		isError  bool
	}{
		{arn: testTrustAnchorARN, region: "us-east-1", endpoint: "https://rolesanywhere.us-east-1.amazonaws.com"},
		{arn: "arn:aws-us-gov:rolesanywhere:us-gov-west-1:123456789012:trust-anchor/abc", region: "us-gov-west-1", endpoint: "https://rolesanywhere.us-gov-west-1.amazonaws.com"},
		{arn: "arn:aws-cn:rolesanywhere:cn-north-1:123456789012:trust-anchor/abc", region: "cn-north-1", endpoint: "https://rolesanywhere.cn-north-1.amazonaws.com.cn"},
		{arn: "arn:aws:iam::123456789012:role/some-role", isError: true},
		{arn: "not-an-arn", isError: true},
		{arn: "arn:aws:rolesanywhere::123456789012:trust-anchor/abc", isError: true},
	} {
		region, endpoint, err := rolesAnywhereEndpoint(c.arn)
		if c.isError {
			if err == nil {
				t.Errorf("expected error for %q", c.arn)
			}
			continue
		}
		if err != nil {
			t.Errorf("unexpected error for %q: %v", c.arn, err)
			continue
		}
		if region != c.region || endpoint != c.endpoint {
			t.Errorf("arn %q: got (%s, %s), expected (%s, %s)", c.arn, region, endpoint, c.region, c.endpoint)
		}
	}
}

func TestValidateAWSAuth(t *testing.T) {
	completeRA := AWSRolesAnywhereConfig{
		Certificate:    "cert",
		PrivateKey:     "key",
		TrustAnchorARN: "arn:1",
		ProfileARN:     "arn:2",
		RoleARN:        "arn:3",
	}
	for _, c := range []struct {
		name      string
		accessKey string
		secretKey string
		ra        AWSRolesAnywhereConfig
		errPart   string
	}{
		{name: "static keys", accessKey: "ak", secretKey: "sk"},
		{name: "roles anywhere", ra: completeRA},
		{name: "nothing", errPart: "missing access_key"},
		{name: "missing secret", accessKey: "ak", errPart: "missing secret_key"},
		{name: "both methods", accessKey: "ak", secretKey: "sk", ra: completeRA, errPart: "not both"},
		{name: "partial roles anywhere", ra: AWSRolesAnywhereConfig{Certificate: "cert"}, errPart: "missing roles_anywhere.private_key"},
		{name: "roles anywhere no role", ra: AWSRolesAnywhereConfig{Certificate: "cert", PrivateKey: "key", TrustAnchorARN: "a", ProfileARN: "b"}, errPart: "missing roles_anywhere.role_arn"},
	} {
		err := ValidateAWSAuth(c.accessKey, c.secretKey, c.ra)
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

func TestNewAWSCredentialsStatic(t *testing.T) {
	creds, err := NewAWSCredentials("ak", "sk", AWSRolesAnywhereConfig{})
	if err != nil {
		t.Fatalf("NewAWSCredentials: %v", err)
	}
	v, err := creds.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v.AccessKeyID != "ak" || v.SecretAccessKey != "sk" {
		t.Errorf("unexpected static credentials: %+v", v)
	}
}

func TestNewAWSCredentialsBadInputs(t *testing.T) {
	base := AWSRolesAnywhereConfig{
		Certificate:    "-----BEGIN CERTIFICATE-----\nnotvalid\n-----END CERTIFICATE-----",
		PrivateKey:     "-----BEGIN PRIVATE KEY-----\nnotvalid\n-----END PRIVATE KEY-----",
		TrustAnchorARN: testTrustAnchorARN,
		ProfileARN:     testProfileARN,
		RoleARN:        testRoleARN,
	}
	if _, err := NewAWSCredentials("", "", base); err == nil {
		t.Errorf("expected error for invalid certificate PEM")
	}

	pair, _ := makeTestCert(t, "rsa", 11, nil, nil)
	badKey := base
	badKey.Certificate = pair.certPEM
	if _, err := NewAWSCredentials("", "", badKey); err == nil || !strings.Contains(err.Error(), "private_key") {
		t.Errorf("expected private key error, got: %v", err)
	}

	badARN := base
	badARN.Certificate = pair.certPEM
	badARN.PrivateKey = pair.keyPEM
	badARN.TrustAnchorARN = "arn:aws:s3:::bucket"
	if _, err := NewAWSCredentials("", "", badARN); err == nil || !strings.Contains(err.Error(), "trust_anchor_arn") {
		t.Errorf("expected trust anchor ARN error, got: %v", err)
	}
}
