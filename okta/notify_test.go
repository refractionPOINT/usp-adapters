package usp_okta

// flushNotifications waits for the queued OnError deliveries, so a
// test can assert on (or on the absence of) a report right after the call that
// caused it.
func (a *OktaAdapter) flushNotifications() { a.notifier.Flush() }
