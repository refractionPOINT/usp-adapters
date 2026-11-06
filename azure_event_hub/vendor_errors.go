package usp_azure_event_hub

import (
	"errors"
	"strings"

	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/refractionPOINT/usp-adapters/internal/vendorerr"
)

// vendorFailure reports the SDK's or Azure's own error text, prefixed with the
// failing operation, with only credential material redacted. The result still
// unwraps to cause, so errors.Is/As checks keep working.
func vendorFailure(operation string, cause error, secrets ...string) error {
	if cause == nil {
		return errors.New(operation + ": vendor operation stopped without an error")
	}
	return vendorerr.Wrap(operation, cause, secrets...)
}

// connectionSecrets lists the secret values of an Event Hubs connection
// string: its shared access key and signature. The rest of the string (the
// namespace host, key name, hub name) is not secret and is what makes a DNS or
// authorization failure actionable; Redact also hides fragments of every secret
// it is given, so the whole string must not be listed.
func connectionSecrets(connection string) []string {
	var secrets []string
	for _, part := range strings.Split(connection, ";") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "sharedaccesskey", "sharedaccesssignature":
			secrets = append(secrets, value)
		}
	}
	return secrets
}

func (a *EventHubAdapter) vendorFailure(operation string, cause error) error {
	return vendorFailure(operation, cause, connectionSecrets(a.conf.ConnectionString)...)
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
