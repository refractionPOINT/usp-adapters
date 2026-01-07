# Demo Adapter

The Demo adapter generates simulated events for testing and demonstration purposes. It supports multiple scenarios that simulate different types of telemetry data.

## Features

- Generates events on a configurable schedule
- Backfills 1 hour of historical events on startup
- Supports multiple scenarios simulating different data sources
- Can run in simple mode (generic JSON events) or scenario mode (realistic simulated telemetry)

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `client_options` | Standard USP client options | Required |
| `scenario` | Scenario to simulate (see below) | None (simple mode) |
| `polling_interval` | Event interval in seconds (simple mode only) | 60 |

## Usage

### Simple Mode

Generates generic demo events every minute:

```bash
./general demo \
  client_options.identity.installation_key=$IK \
  client_options.identity.oid=$OID \
  client_options.platform=json \
  client_options.sensor_seed_key=demo
```

### Scenario Mode

Generates realistic simulated events matching a specific data source:

```bash
./general demo \
  client_options.identity.installation_key=$IK \
  client_options.identity.oid=$OID \
  client_options.platform=json \
  client_options.sensor_seed_key=demo \
  scenario=wel_idle
```

## Available Scenarios

### `wel_idle`

Simulates Windows Event Logs from an idle Windows 10 system.

**Event Types Generated:**

| Event | Channel | Event ID | Interval | Description |
|-------|---------|----------|----------|-------------|
| System Logon | Security | 4624 | 5 min | Service account logons |
| Logoff | Security | 4634 | 7 min | Logoff events |
| Special Privileges | Security | 4672 | 5 min | Privilege assignments |
| Service Control Manager | System | 7036 | 10 min | Service state changes |
| Time Sync | System | 37 | 15 min | Time synchronization |
| Application Info | Application | 1000 | 3 min | Background tasks |

**Example Event:**

```json
{
  "EVENT": {
    "System": {
      "Channel": "Security",
      "Computer": "DESKTOP-DEMO123",
      "EventID": "4624",
      "Provider": {
        "Name": "Microsoft-Windows-Security-Auditing",
        "Guid": "{54849625-5478-4994-A5BA-3E3B0328C30D}"
      },
      "TimeCreated": {
        "SystemTime": "2026-01-07T14:30:00.0000000Z"
      },
      "_event_id": "4624"
    },
    "EventData": {
      "TargetUserName": "SYSTEM",
      "TargetDomainName": "NT AUTHORITY",
      "LogonType": "5"
    }
  }
}
```

## Adding New Scenarios

Create a new file in `demo/scenarios/` following this pattern:

```go
package scenarios

import "time"

func init() {
    Register(myScenario())
}

func myScenario() *Scenario {
    return &Scenario{
        Name:        "my_scenario",
        Description: "Description of what this simulates",
        Platform:    "json", // or "text"
        Generators: []EventGenerator{
            {
                Name:     "EventName",
                Interval: 5 * time.Minute,
                Generate: func(eventTime time.Time, sequence int) Event {
                    return Event{
                        JsonPayload: map[string]interface{}{
                            "field": "value",
                        },
                        Timestamp: eventTime,
                        EventType: "MY_EVENT_TYPE",
                    }
                },
            },
        },
    }
}
```

### Event Structure

```go
type Event struct {
    TextPayload string                 // For text/XML events
    JsonPayload map[string]interface{} // For JSON events
    Timestamp   time.Time              // Event timestamp
    EventType   string                 // Routing event type (e.g., "WEL")
}
```

### Generator Structure

```go
type EventGenerator struct {
    Name     string                                        // For logging
    Interval time.Duration                                 // Generation frequency
    Generate func(eventTime time.Time, sequence int) Event // Event factory
}
```

## Behavior

1. **On Startup**: The adapter calculates how many events would have occurred in the last hour and generates backfill events with appropriate historical timestamps.

2. **During Operation**: Each generator runs independently at its configured interval, producing events in real-time.

3. **Shutdown**: Gracefully drains any pending events before closing.
