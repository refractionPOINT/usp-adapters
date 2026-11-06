package usp_defender

// flushNotifications waits for the OnError deliveries notify has started, so a
// test can assert on (or on the absence of) a report right after the call that
// caused it.
func (a *DefenderAdapter) flushNotifications() { a.notifying.Wait() }
