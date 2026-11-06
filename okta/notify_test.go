package usp_okta

// flushNotifications waits for the OnError deliveries notify has started, so a
// test can assert on (or on the absence of) a report right after the call that
// caused it.
func (a *OktaAdapter) flushNotifications() { a.notifying.Wait() }
