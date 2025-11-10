package utils

import (
	"fmt"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

// Shipper is an interface for shipping data messages to the cloud.
// This interface abstracts the core functionality of uspclient.Client and
// enables transparent wrapping for filtering and other middleware operations.
//
// Both uspclient.Client and FilteredClient implement this interface, allowing
// adapters to use either without code changes. This design pattern enables
// composable behavior chains (filtering, rate limiting, etc.) while maintaining
// a simple, consistent API.
//
// Implementations must be thread-safe and handle concurrent calls to Ship().
type Shipper interface {
	// Ship sends a single message to the cloud with the specified timeout.
	// Returns an error if the message cannot be sent within the timeout period.
	// Thread-safe: multiple goroutines may call Ship() concurrently.
	//
	// Parameters:
	//   - msg: The data message to send
	//   - timeout: Maximum duration to wait for the send operation
	//
	// Returns:
	//   - nil on success
	//   - error if send fails or times out
	Ship(msg *protocol.DataMessage, timeout time.Duration) error

	// Drain flushes any pending messages in the queue and waits for them to be sent.
	// This should be called during graceful shutdown before Close().
	// Thread-safe: safe to call from multiple goroutines, but typically called once.
	//
	// Parameters:
	//   - timeout: Maximum duration to wait for draining to complete
	//
	// Returns:
	//   - nil if all pending messages are sent within timeout
	//   - error if timeout occurs or draining fails
	Drain(timeout time.Duration) error

	// Close terminates the client connection and releases all resources.
	// Any messages that couldn't be sent are returned for potential retry.
	// After Close() is called, Ship() and Drain() must not be called.
	//
	// Returns:
	//   - slice of unsent messages that were still queued
	//   - error if close operation fails
	//
	// Note: Implementations should be idempotent - calling Close() multiple
	// times should not panic or cause errors.
	Close() ([]*protocol.DataMessage, error)
}

// FilteredClient wraps a USP client and filters messages before shipping.
type FilteredClient struct {
	client       *uspclient.Client
	filterEngine *FilterEngine
	debugLog     LogFunc
}

// NewFilteredClient creates a new filtered client wrapper.
func NewFilteredClient(client *uspclient.Client, patterns []FilterPattern, debugLog LogFunc) (*FilteredClient, error) {
	if debugLog == nil {
		debugLog = func(string) {} // No-op logger
	}

	filterEngine, err := NewFilterEngine(patterns, debugLog)
	if err != nil {
		return nil, err
	}

	return &FilteredClient{
		client:       client,
		filterEngine: filterEngine,
		debugLog:     debugLog,
	}, nil
}

// Ship sends a message through the filter, then to the client if not filtered.
func (fc *FilteredClient) Ship(msg *protocol.DataMessage, timeout time.Duration) error {
	shouldFilter, pattern := fc.filterEngine.ShouldFilter(msg)
	if shouldFilter {
		// Determine payload type for logging
		payloadType := "unknown"
		if msg.TextPayload != "" {
			payloadType = "TextPayload"
		} else if msg.JsonPayload != nil {
			payloadType = "JsonPayload"
		}

		fc.debugLog(fmt.Sprintf("Filtered: matched pattern %q in %s", pattern, payloadType))
		return nil // Filtered out, not an error
	}

	// Not filtered, ship it
	return fc.client.Ship(msg, timeout)
}

// Drain closes the filter engine (logs final stats) and drains the underlying client.
func (fc *FilteredClient) Drain(timeout time.Duration) error {
	fc.filterEngine.Close()
	return fc.client.Drain(timeout)
}

// Close closes the filter engine and the underlying client.
func (fc *FilteredClient) Close() ([]*protocol.DataMessage, error) {
	fc.filterEngine.Close()
	return fc.client.Close()
}
