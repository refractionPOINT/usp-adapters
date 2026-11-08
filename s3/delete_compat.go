package usp_s3

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// Only an explicit unsupported-operation response can weaken source identity
// protection. In particular, preconditions, permissions and transport failures
// must never turn into unconditional deletion. Each request needs its own
// evidence: an endpoint-wide capability cache could bypass a later 412/403.
func unsupportedConditionalDelete(err error) bool {
	var response awserr.RequestFailure
	status := 0
	if errors.As(err, &response) {
		status = response.StatusCode()
		switch status {
		case http.StatusNotImplemented:
			return true
		case http.StatusBadRequest, http.StatusMethodNotAllowed, http.StatusUnprocessableEntity:
			// These statuses alone are insufficient; require explicit SDK error detail.
		default:
			return false
		}
	}
	var serviceError awserr.Error
	if !errors.As(err, &serviceError) {
		return false
	}
	code := serviceError.Code()
	if code == "NotImplemented" {
		return true
	}
	message := strings.ToLower(serviceError.Message())
	message = strings.NewReplacer("if-match", "ifmatch", "if_match", "ifmatch", "if match", "ifmatch").Replace(message)
	conditional := strings.Contains(message, "ifmatch") || strings.Contains(message, "conditional delete") || strings.Contains(message, "conditional requests")
	if !conditional {
		return false
	}
	switch code {
	case "UnsupportedHeader", "UnsupportedHeaders", "UnsupportedOperation", "UnsupportedFeature", "NotSupported":
		return true
	case "InvalidRequest", "InvalidArgument":
		return strings.Contains(message, "not implemented") || strings.Contains(message, "not supported") || strings.Contains(message, "unsupported") || strings.Contains(message, "does not support") || strings.Contains(message, "unknown header") || strings.Contains(message, "unrecognized header")
	default:
		return false
	}
}

// Called only by the collector (or Close after joining it), after cleanup joins.
// Workers never invoke warning callbacks concurrently. Count attempts even when
// the plain request fails; its genuine failure still follows the sticky path.
func (a *S3Adapter) warnDeleteCompatibilityFallbacks() {
	total := a.conditionalDeleteFallbacks.Load()
	if total == a.warnedDeleteFallbacks {
		return
	}
	count := total - a.warnedDeleteFallbacks
	a.warnedDeleteFallbacks = total
	if a.conf.ClientOptions.OnWarning != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("s3: conditional DELETE unsupported; unconditional key DELETE compatibility fallback count=%d total=%d after confirmed delivery; replacement protection is reduced", count, total))
	}
}
