package usp_azure_event_hub

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"strings"

	amqp "github.com/Azure/go-amqp"
	"github.com/mitchellh/mapstructure"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
)

// Return a new error, not a wrapper retaining the potentially sensitive raw
// cause. Only known fixed parser messages, protocol conditions and diagnostic
// categories cross the callback boundary. Descriptions, URLs, addresses, SAS
// values, event annotations and payloads are never copied into this error.
func vendorFailure(operation string, cause error) error {
	return errors.New(operation + ": " + safeVendorCause(cause))
}
func safeVendorCause(err error) string {
	if err == nil {
		return "vendor operation stopped without a verified cause"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context deadline exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "context canceled"
	}
	if errors.Is(err, uspclient.ErrorBufferFull) {
		return "USP buffer full"
	}
	var protocolErr *amqp.Error
	if errors.As(err, &protocolErr) {
		switch protocolErr.Condition {
		case amqp.ErrCondUnauthorizedAccess, amqp.ErrCondNotFound, amqp.ErrCondNotAllowed,
			amqp.ErrCondResourceLimitExceeded, amqp.ErrCondResourceLocked, amqp.ErrCondInternalError,
			amqp.ErrCondConnectionForced, amqp.ErrCondDetachForced, amqp.ErrCondDecodeError,
			amqp.ErrCondInvalidField, amqp.ErrCondNotImplemented, amqp.ErrCondPreconditionFailed:
			return string(protocolErr.Condition)
		default:
			return "AMQP protocol error"
		}
	}
	var decode *mapstructure.Error
	if errors.As(err, &decode) {
		return "decode failure: invalid event annotations"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "name resolution failed"
	}
	var unknownCA x509.UnknownAuthorityError
	var invalidCert x509.CertificateInvalidError
	var hostname x509.HostnameError
	if errors.As(err, &unknownCA) || errors.As(err, &invalidCert) || errors.As(err, &hostname) {
		return "TLS certificate validation failed"
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return "connection timed out"
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return "network connection failed"
	}
	var link *amqp.LinkError
	if errors.As(err, &link) {
		return "AMQP link closed"
	}
	var session *amqp.SessionError
	if errors.As(err, &session) {
		return "AMQP session closed"
	}
	var connection *amqp.ConnError
	if errors.As(err, &connection) {
		return "AMQP connection closed"
	}
	// conn.ParsedConnectionFromStr emits these fixed messages. Equality (never
	// prefix matching) prevents a credential-bearing appended description escaping.
	text := err.Error()
	switch text {
	case "failed parsing connection string due to unmatched key value separated by '='",
		"failed parsing connection string due to an incorrectly formatted Endpoint value",
		"failed parsing connection string due to Endpoint value not containing a URL with a namespace and a suffix",
		`key "Endpoint" must not be empty`, `key "SharedAccessKeyName" must not be empty`, `key "SharedAccessKey" must not be empty`, `key "EntityPath" must not be empty`,
		"decode failure", "invalid event metadata":
		return text
	}
	// Some SDK CBS/authentication errors have no public typed cause. Classify
	// their diagnostic category, but never echo arbitrary error text.
	if len(text) <= 4096 {
		lower := strings.ToLower(text)
		switch {
		case strings.Contains(lower, "unauthorized"), strings.Contains(lower, "unauthorised"):
			return "authorization failed"
		case strings.Contains(lower, "authentication"), strings.Contains(lower, "signature validation"), strings.Contains(lower, "cbs token"):
			return "authentication failed"
		case strings.Contains(lower, "decode"), strings.Contains(lower, "decoding"):
			return "decode failure: invalid event annotations"
		case strings.Contains(lower, "connection reset"), strings.Contains(lower, "connection refused"):
			return "network connection failed"
		}
	}
	return "vendor operation failed (unclassified cause)"
}

func collectionInternalError(err error) bool {
	for _, internal := range []error{acktrack.ErrSequence, acktrack.ErrDependency, acktrack.ErrLimit, errPositionInvalid} {
		if errors.Is(err, internal) {
			return true
		}
	}
	return false
}

var errPositionInvalid = errors.New("event hub position invalid")
