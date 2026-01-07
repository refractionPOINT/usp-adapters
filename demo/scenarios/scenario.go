package scenarios

import (
	"time"
)

// Event represents an event to be shipped
type Event struct {
	// TextPayload is used for text-based events (like WEL XML)
	TextPayload string
	// JsonPayload is used for JSON-based events
	JsonPayload map[string]interface{}
	// Timestamp is the event time
	Timestamp time.Time
	// EventType is the routing event type (e.g., "WEL", "json")
	EventType string
}

// EventGenerator defines a specific type of event that can be generated
type EventGenerator struct {
	// Name identifies this generator (for logging)
	Name string
	// Interval is how often this event type occurs
	Interval time.Duration
	// Generate creates an event at the given time
	Generate func(eventTime time.Time, sequence int) Event
}

// Scenario defines a collection of event generators simulating a specific use case
type Scenario struct {
	// Name is the scenario identifier used in config
	Name string
	// Description explains what this scenario simulates
	Description string
	// Platform is the data format (e.g., "text" for XML, "json" for JSON)
	Platform string
	// Generators are the event generators for this scenario
	Generators []EventGenerator
}

// Registry holds all available scenarios
var Registry = make(map[string]*Scenario)

// Register adds a scenario to the registry
func Register(s *Scenario) {
	Registry[s.Name] = s
}

// Get retrieves a scenario by name
func Get(name string) *Scenario {
	return Registry[name]
}

// List returns all available scenario names
func List() []string {
	names := make([]string, 0, len(Registry))
	for name := range Registry {
		names = append(names, name)
	}
	return names
}
