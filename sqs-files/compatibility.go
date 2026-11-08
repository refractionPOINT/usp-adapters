package usp_sqs_files

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strconv"
	"sync"
	"time"
)

// Permission fallback is shared for one adapter lifetime. Disable only this
// optional operation, warn once, and retain batches that outlive their original
// lease without draining/resetting the SDK or stopping the collector.
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
			c.warning("renewal_disabled_count=1 reason=permission_denied; using original SQS visibility deadline; expired batches retained for source replay")
		}
	})
}

var errOriginalLeaseRetained = errors.New("original SQS visibility deadline expired in permission compatibility mode; retained for source replay")

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
func (l *batchLease) compatibilityExpired() bool {
	return l.compatibility.isDisabled() && !time.Now().Before(minTime(l.originalDeadline, l.hardDeadline))
}
func (l *batchLease) expire() error {
	err := error(context.DeadlineExceeded)
	if l.compatibility.isDisabled() {
		err = errOriginalLeaseRetained
	}
	l.cancel(err)
	return err
}
func (l *batchLease) waitOriginalLease() {
	deadline := minTime(l.originalDeadline, l.hardDeadline)
	if !time.Now().Before(deadline) {
		l.expire()
		return
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-l.stopRequested:
	case <-l.renewCtx.Done():
	case <-timer.C:
		l.expire()
	}
}
