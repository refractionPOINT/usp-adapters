package utils

import "errors"

// ErrPersistentVendorFailure marks an error delivered to ClientOptions.OnError
// as advisory: the vendor has been failing for a long continuous interval, but
// the adapter is still running, keeps retrying from its current cursor and will
// catch up once the vendor recovers. Embedders should surface such an error
// (log it, show it to the user) and must NOT stop or restart the adapter in
// response: a restarted adapter may begin at "now" or at the latest offset and
// permanently skip everything produced during the outage.
//
// Test with errors.Is(err, utils.ErrPersistentVendorFailure). Any other OnError
// error is not covered by this guarantee and should be treated as before.
var ErrPersistentVendorFailure = errors.New("persistent vendor failure; adapter still running and retrying")

// persistentFailure keeps the wrapped error's text unchanged (it is already
// the vendor's redacted text) while unwrapping to the sentinel and the cause.
type persistentFailure struct{ err error }

func (e *persistentFailure) Error() string   { return e.err.Error() }
func (e *persistentFailure) Unwrap() []error { return []error{ErrPersistentVendorFailure, e.err} }

// PersistentVendorFailure marks err as an advisory persistent-failure report:
// errors.Is(result, ErrPersistentVendorFailure) is true, Error() is unchanged
// and the original chain stays reachable. A nil err returns nil.
func PersistentVendorFailure(err error) error {
	if err == nil {
		return nil
	}
	return &persistentFailure{err: err}
}
