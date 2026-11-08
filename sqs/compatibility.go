package usp_sqs

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Permission fallback is shared for one adapter lifetime. Disable only this
// optional operation and warn once. Native no-renew work and receipt waiting
// continue under caller cancellation without claiming ownership was extended.
type renewalCompatibility struct {
	disabled chan struct{}
	once     sync.Once
	warning  func(string)
}

func newRenewalCompatibility(warning func(string)) *renewalCompatibility {
	return &renewalCompatibility{disabled: make(chan struct{}), warning: warning}
}
func (c *renewalCompatibility) isDisabled() bool {
	select {
	case <-c.disabled:
		return true
	default:
		return false
	}
}
func (c *renewalCompatibility) disable() {
	c.once.Do(func() {
		close(c.disabled)
		if c.warning != nil {
			c.warning("renewal_disabled_count=1 reason=permission_denied; native no-renew work continues until delivery receipts; expired handles may replay")
		}
	})
}

func permissionDeniedCode(code string) bool {
	switch code {
	case "AccessDenied", "AccessDeniedException", "AccessDeniedFault", "Forbidden", "UnauthorizedOperation":
		return true
	}
	return false
}
func renewalPermissionDenied(out *sqs.ChangeMessageVisibilityBatchOutput, err error, n int) bool {
	if err != nil {
		var failure awserr.RequestFailure
		if errors.As(err, &failure) {
			if failure.StatusCode() == 403 {
				return true
			}
			if failure.StatusCode() >= 500 || failure.StatusCode() == 429 {
				return false
			}
		}
		var service awserr.Error
		return errors.As(err, &service) && permissionDeniedCode(service.Code())
	}
	if out == nil || len(out.Failed) == 0 || len(out.Failed)+len(out.Successful) != n {
		return false
	}
	seen := make(map[string]bool, n)
	valid := func(id *string) bool {
		if id == nil || seen[*id] {
			return false
		}
		i, e := strconv.Atoi(*id)
		if e != nil || i < 0 || i >= n || strconv.Itoa(i) != *id {
			return false
		}
		seen[*id] = true
		return true
	}
	for _, entry := range out.Successful {
		if entry == nil || !valid(entry.Id) {
			return false
		}
	}
	for _, entry := range out.Failed {
		if entry == nil || !valid(entry.Id) || !permissionDeniedCode(aws.StringValue(entry.Code)) {
			return false
		}
	}
	return true
}
func (l *batchLease) knownDeadline() time.Time {
	l.mu.Lock()
	deadline := l.deadline
	l.mu.Unlock()
	deadline = minTime(deadline, l.hardDeadline)
	if l.compatibility.isDisabled() {
		deadline = minTime(deadline, l.originalDeadline)
	}
	return deadline
}

// Expiry ends work only while ownership is required by the normal renewal
// path. Permission fallback keeps the original known deadline as evidence,
// but does not use source visibility as a processing/receipt-wait deadline.
func (l *batchLease) expire() error {
	if l.compatibility.isDisabled() {
		return nil
	}
	l.cancel(context.DeadlineExceeded)
	return context.DeadlineExceeded
}

func staleReceiptCode(code, message string) bool {
	switch code {
	case "ReceiptHandleIsInvalid", "InvalidIdFormat":
		return true
	case "InvalidParameterValue":
		message = strings.ToLower(message)
		return (strings.Contains(message, "receipt handle") || strings.Contains(message, "receipthandle")) &&
			(strings.Contains(message, "expired") || strings.Contains(message, "invalid"))
	}
	return false
}

// Only definitive stale-handle results in native no-renew mode are nonfatal.
// A mixed response containing any real source failure remains fatal. Validate
// every result ID before counting entries; never log handles or source messages.
func (c *renewalCompatibility) retainStaleDelete(out *sqs.DeleteMessageBatchOutput, err error, n int) bool {
	if !c.isDisabled() || n <= 0 {
		return false
	}
	count := 0
	if err != nil {
		var failure awserr.RequestFailure
		if errors.As(err, &failure) && failure.StatusCode() != 400 {
			return false
		}
		var service awserr.Error
		if !errors.As(err, &service) || !staleReceiptCode(service.Code(), service.Message()) {
			return false
		}
		count = n
	} else {
		if out == nil || len(out.Failed) == 0 || len(out.Failed)+len(out.Successful) != n {
			return false
		}
		seen := make(map[string]bool, n)
		valid := func(id *string) bool {
			if id == nil || seen[*id] {
				return false
			}
			i, e := strconv.Atoi(*id)
			if e != nil || i < 0 || i >= n || strconv.Itoa(i) != *id {
				return false
			}
			seen[*id] = true
			return true
		}
		for _, entry := range out.Successful {
			if entry == nil || !valid(entry.Id) {
				return false
			}
		}
		for _, entry := range out.Failed {
			if entry == nil || !valid(entry.Id) || !staleReceiptCode(aws.StringValue(entry.Code), aws.StringValue(entry.Message)) {
				return false
			}
			count++
		}
	}
	if c.warning != nil {
		c.warning(fmt.Sprintf("delete_retained_count=%d reason=expired_or_invalid_receipt_handle; delivered records retained for source replay", count))
	}
	return true
}
