package utils

import (
	"fmt"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

// Shipper is an interface for shipping data messages.
// This interface is implemented by both uspclient.Client and FilteredClient,
// allowing transparent wrapping for filtering.
type Shipper interface {
	Ship(msg *protocol.DataMessage, timeout time.Duration) error
	Drain(timeout time.Duration) error
	Close() ([]*protocol.DataMessage, error)
}

// FilteredClient wraps a USP client and filters messages before shipping.
type FilteredClient struct {
	client       *uspclient.Client
	filterEngine *FilterEngine
	debugLog     LogFunc
}

// NewFilteredClient creates a new filtered client wrapper.
func NewFilteredClient(client *uspclient.Client, patterns []string, debugLog LogFunc) (*FilteredClient, error) {
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
