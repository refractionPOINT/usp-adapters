package utils

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
)

// AWSRolesAnywhereConfig holds the parameters needed to obtain temporary
// AWS credentials through IAM Roles Anywhere instead of long-lived
// access keys. The certificate and private key are PEM encoded; the
// certificate value may contain the full chain, leaf first.
type AWSRolesAnywhereConfig struct {
	Certificate    string `json:"certificate,omitempty" yaml:"certificate,omitempty"`
	PrivateKey     string `json:"private_key,omitempty" yaml:"private_key,omitempty"`
	TrustAnchorARN string `json:"trust_anchor_arn,omitempty" yaml:"trust_anchor_arn,omitempty"`
	ProfileARN     string `json:"profile_arn,omitempty" yaml:"profile_arn,omitempty"`
	RoleARN        string `json:"role_arn,omitempty" yaml:"role_arn,omitempty"`
}

// IsEnabled reports whether the user provided any Roles Anywhere
// parameter at all, which selects this authentication method.
func (c AWSRolesAnywhereConfig) IsEnabled() bool {
	return c.Certificate != "" ||
		c.PrivateKey != "" ||
		c.TrustAnchorARN != "" ||
		c.ProfileARN != "" ||
		c.RoleARN != ""
}

func (c AWSRolesAnywhereConfig) Validate() error {
	if c.Certificate == "" {
		return errors.New("missing roles_anywhere.certificate")
	}
	if c.PrivateKey == "" {
		return errors.New("missing roles_anywhere.private_key")
	}
	if c.TrustAnchorARN == "" {
		return errors.New("missing roles_anywhere.trust_anchor_arn")
	}
	if c.ProfileARN == "" {
		return errors.New("missing roles_anywhere.profile_arn")
	}
	if c.RoleARN == "" {
		return errors.New("missing roles_anywhere.role_arn")
	}
	return nil
}

// ValidateAWSAuth validates that exactly one of the two supported AWS
// authentication methods is fully configured: static access keys or
// IAM Roles Anywhere.
func ValidateAWSAuth(accessKey string, secretKey string, ra AWSRolesAnywhereConfig) error {
	if ra.IsEnabled() {
		if accessKey != "" || secretKey != "" {
			return errors.New("provide either access_key/secret_key or roles_anywhere, not both")
		}
		return ra.Validate()
	}
	if accessKey == "" {
		return errors.New("missing access_key")
	}
	if secretKey == "" {
		return errors.New("missing secret_key")
	}
	return nil
}

// NewAWSCredentials returns aws-sdk-go credentials backed by either the
// static access keys or, when configured, IAM Roles Anywhere.
func NewAWSCredentials(accessKey string, secretKey string, ra AWSRolesAnywhereConfig) (*credentials.Credentials, error) {
	if !ra.IsEnabled() {
		return credentials.NewStaticCredentials(accessKey, secretKey, ""), nil
	}
	p, err := newRolesAnywhereProvider(ra)
	if err != nil {
		return nil, err
	}
	return credentials.NewCredentials(p), nil
}

const (
	rolesAnywhereProviderName   = "AWSRolesAnywhere"
	rolesAnywhereSessionSeconds = 3600
	// Refresh credentials a bit before they actually expire so requests
	// in flight never race the expiration.
	rolesAnywhereExpiryWindow = 5 * time.Minute
)

// rolesAnywhereProvider implements credentials.Provider by calling the
// Roles Anywhere CreateSession API, authenticated with the configured
// X.509 certificate. CreateSession uses a SigV4 variant (SigV4-X509)
// that no AWS SDK implements natively, so the signing is done here.
// Reference: https://docs.aws.amazon.com/rolesanywhere/latest/userguide/authentication-sign-process.html
type rolesAnywhereProvider struct {
	credentials.Expiry

	conf AWSRolesAnywhereConfig

	cert      *x509.Certificate
	chain     []*x509.Certificate
	key       crypto.Signer
	algorithm string

	region   string
	endpoint string

	httpClient *http.Client
	now        func() time.Time
}

func newRolesAnywhereProvider(conf AWSRolesAnywhereConfig) (*rolesAnywhereProvider, error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}
	cert, chain, err := parsePEMCertificates(conf.Certificate)
	if err != nil {
		return nil, fmt.Errorf("roles_anywhere.certificate: %v", err)
	}
	key, algorithm, err := parsePEMPrivateKey(conf.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("roles_anywhere.private_key: %v", err)
	}
	region, endpoint, err := rolesAnywhereEndpoint(conf.TrustAnchorARN)
	if err != nil {
		return nil, fmt.Errorf("roles_anywhere.trust_anchor_arn: %v", err)
	}
	return &rolesAnywhereProvider{
		conf:       conf,
		cert:       cert,
		chain:      chain,
		key:        key,
		algorithm:  algorithm,
		region:     region,
		endpoint:   endpoint,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		now:        time.Now,
	}, nil
}

func (p *rolesAnywhereProvider) Retrieve() (credentials.Value, error) {
	body, err := json.Marshal(map[string]interface{}{
		"durationSeconds": rolesAnywhereSessionSeconds,
		"profileArn":      p.conf.ProfileARN,
		"roleArn":         p.conf.RoleARN,
		"trustAnchorArn":  p.conf.TrustAnchorARN,
	})
	if err != nil {
		return credentials.Value{}, err
	}

	u, err := url.Parse(p.endpoint)
	if err != nil {
		return credentials.Value{}, fmt.Errorf("rolesanywhere endpoint: %v", err)
	}

	now := p.now().UTC()
	amzDate := now.Format("20060102T150405Z")
	scope := fmt.Sprintf("%s/%s/rolesanywhere/aws4_request", now.Format("20060102"), p.region)

	// Headers included in the signature, already in the sorted order
	// required by the canonical request.
	headers := [][2]string{
		{"content-type", "application/json"},
		{"host", u.Host},
		{"x-amz-date", amzDate},
		{"x-amz-x509", base64.StdEncoding.EncodeToString(p.cert.Raw)},
	}
	if len(p.chain) != 0 {
		encoded := make([]string, 0, len(p.chain))
		for _, c := range p.chain {
			encoded = append(encoded, base64.StdEncoding.EncodeToString(c.Raw))
		}
		headers = append(headers, [2]string{"x-amz-x509-chain", strings.Join(encoded, ",")})
	}

	canonicalHeaders := strings.Builder{}
	signedNames := make([]string, 0, len(headers))
	for _, h := range headers {
		canonicalHeaders.WriteString(h[0] + ":" + h[1] + "\n")
		signedNames = append(signedNames, h[0])
	}
	signedHeaders := strings.Join(signedNames, ";")

	payloadHash := sha256.Sum256(body)
	canonicalRequest := strings.Join([]string{
		"POST",
		"/sessions",
		"", // no query string
		canonicalHeaders.String(),
		signedHeaders,
		hex.EncodeToString(payloadHash[:]),
	}, "\n")

	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		p.algorithm,
		amzDate,
		scope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")

	digest := sha256.Sum256([]byte(stringToSign))
	var signature []byte
	switch k := p.key.(type) {
	case *rsa.PrivateKey:
		signature, err = rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, digest[:])
	case *ecdsa.PrivateKey:
		signature, err = ecdsa.SignASN1(rand.Reader, k, digest[:])
	default:
		err = fmt.Errorf("unsupported private key type %T", p.key)
	}
	if err != nil {
		return credentials.Value{}, fmt.Errorf("rolesanywhere signing: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, p.endpoint+"/sessions", strings.NewReader(string(body)))
	if err != nil {
		return credentials.Value{}, err
	}
	for _, h := range headers {
		if h[0] == "host" {
			continue
		}
		req.Header.Set(h[0], h[1])
	}
	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		p.algorithm, p.cert.SerialNumber.String(), scope, signedHeaders, hex.EncodeToString(signature)))

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return credentials.Value{}, fmt.Errorf("rolesanywhere CreateSession: %v", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return credentials.Value{}, fmt.Errorf("rolesanywhere CreateSession: %v", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return credentials.Value{}, fmt.Errorf("rolesanywhere CreateSession status %d: %s", resp.StatusCode, string(respBody))
	}

	session := struct {
		CredentialSet []struct {
			Credentials struct {
				AccessKeyID     string `json:"accessKeyId"`
				SecretAccessKey string `json:"secretAccessKey"`
				SessionToken    string `json:"sessionToken"`
				Expiration      string `json:"expiration"`
			} `json:"credentials"`
		} `json:"credentialSet"`
	}{}
	if err := json.Unmarshal(respBody, &session); err != nil {
		return credentials.Value{}, fmt.Errorf("rolesanywhere CreateSession response: %v", err)
	}
	if len(session.CredentialSet) == 0 {
		return credentials.Value{}, errors.New("rolesanywhere CreateSession response: empty credentialSet")
	}
	creds := session.CredentialSet[0].Credentials
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		return credentials.Value{}, errors.New("rolesanywhere CreateSession response: missing credentials")
	}

	expiration, err := time.Parse(time.RFC3339, creds.Expiration)
	if err != nil {
		return credentials.Value{}, fmt.Errorf("rolesanywhere CreateSession expiration: %v", err)
	}
	window := rolesAnywhereExpiryWindow
	if remaining := expiration.Sub(now); remaining < 2*window {
		window = remaining / 2
	}
	p.SetExpiration(expiration, window)

	return credentials.Value{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		ProviderName:    rolesAnywhereProviderName,
	}, nil
}

// rolesAnywhereEndpoint derives the region and API endpoint from the
// trust anchor ARN, like:
// arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/uuid
func rolesAnywhereEndpoint(trustAnchorARN string) (string, string, error) {
	parts := strings.Split(trustAnchorARN, ":")
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "rolesanywhere" || parts[3] == "" {
		return "", "", fmt.Errorf("invalid trust anchor ARN %q", trustAnchorARN)
	}
	region := parts[3]
	domain := "amazonaws.com"
	if parts[1] == "aws-cn" {
		domain = "amazonaws.com.cn"
	}
	return region, fmt.Sprintf("https://rolesanywhere.%s.%s", region, domain), nil
}

// normalizePEM allows PEM values pasted as a single line with literal
// "\n" sequences, which is common when configs are edited in a UI.
// Backslashes never appear in real PEM content so this is safe.
func normalizePEM(data string) []byte {
	return []byte(strings.TrimSpace(strings.ReplaceAll(data, "\\n", "\n")))
}

// parsePEMCertificates returns the leaf certificate and, if the PEM
// contains more than one certificate, the rest of the chain in order.
func parsePEMCertificates(pemData string) (*x509.Certificate, []*x509.Certificate, error) {
	rest := normalizePEM(pemData)
	certs := []*x509.Certificate{}
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid certificate: %v", err)
		}
		certs = append(certs, c)
	}
	if len(certs) == 0 {
		return nil, nil, errors.New("no certificate found in PEM data")
	}
	return certs[0], certs[1:], nil
}

// parsePEMPrivateKey parses an RSA or ECDSA private key in PKCS#8,
// PKCS#1 or SEC1 PEM form and returns it along with the matching
// Roles Anywhere signing algorithm identifier.
func parsePEMPrivateKey(pemData string) (crypto.Signer, string, error) {
	block, _ := pem.Decode(normalizePEM(pemData))
	if block == nil {
		return nil, "", errors.New("no private key found in PEM data")
	}
	var key interface{}
	var err error
	if key, err = x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
		if key, err = x509.ParsePKCS1PrivateKey(block.Bytes); err != nil {
			if key, err = x509.ParseECPrivateKey(block.Bytes); err != nil {
				return nil, "", errors.New("invalid private key: expecting PKCS#8, PKCS#1 or SEC1 PEM")
			}
		}
	}
	switch k := key.(type) {
	case *rsa.PrivateKey:
		return k, "AWS4-X509-RSA-SHA256", nil
	case *ecdsa.PrivateKey:
		return k, "AWS4-X509-ECDSA-SHA256", nil
	default:
		return nil, "", fmt.Errorf("unsupported private key type %T, expecting RSA or ECDSA", key)
	}
}
