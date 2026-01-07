package scenarios

import (
	"fmt"
	"math/rand"
	"time"
)

func init() {
	Register(welIdleScenario())
}

func welIdleScenario() *Scenario {
	return &Scenario{
		Name:        "wel_idle",
		Description: "Simulates Windows Event Logs from an idle Windows 10 system",
		Platform:    "json",
		Generators: []EventGenerator{
			// Security: System service logon (every 5 minutes)
			{
				Name:     "Security-4624-SystemLogon",
				Interval: 5 * time.Minute,
				Generate: generateSecurityLogon,
			},
			// Security: Logoff events (every 7 minutes)
			{
				Name:     "Security-4634-Logoff",
				Interval: 7 * time.Minute,
				Generate: generateSecurityLogoff,
			},
			// Security: Special privileges assigned (every 5 minutes)
			{
				Name:     "Security-4672-SpecialPrivileges",
				Interval: 5 * time.Minute,
				Generate: generateSecuritySpecialPrivileges,
			},
			// System: Service Control Manager (every 10 minutes)
			{
				Name:     "System-7036-SCM",
				Interval: 10 * time.Minute,
				Generate: generateSystemSCM,
			},
			// System: Time sync (every 15 minutes)
			{
				Name:     "System-37-TimeSync",
				Interval: 15 * time.Minute,
				Generate: generateSystemTimeSync,
			},
			// Application: Background task (every 3 minutes)
			{
				Name:     "Application-1000-AppInfo",
				Interval: 3 * time.Minute,
				Generate: generateApplicationInfo,
			},
		},
	}
}

const computerName = "DESKTOP-DEMO123"

var systemAccounts = []string{
	"SYSTEM",
	"LOCAL SERVICE",
	"NETWORK SERVICE",
}

var services = []struct {
	Name        string
	DisplayName string
}{
	{"wuauserv", "Windows Update"},
	{"WSearch", "Windows Search"},
	{"Spooler", "Print Spooler"},
	{"BITS", "Background Intelligent Transfer Service"},
	{"Themes", "Themes"},
	{"Schedule", "Task Scheduler"},
	{"Dhcp", "DHCP Client"},
	{"Dnscache", "DNS Client"},
}

// Helper to create the System block common to all WEL events
func makeSystemBlock(eventTime time.Time, sequence int, providerName, providerGuid, channel string, eventID int, level, task, opcode, version int, keywords string) map[string]interface{} {
	recordID := (eventID * 100000) + sequence

	system := map[string]interface{}{
		"Channel":       channel,
		"Computer":      computerName,
		"Correlation":   "",
		"EventID":       fmt.Sprintf("%d", eventID),
		"EventRecordID": fmt.Sprintf("%d", recordID),
		"Execution": map[string]interface{}{
			"ProcessID": "636",
			"ThreadID":  "4660",
		},
		"Keywords": keywords,
		"Level":    fmt.Sprintf("%d", level),
		"Opcode":   fmt.Sprintf("%d", opcode),
		"Provider": map[string]interface{}{
			"Name": providerName,
			"Guid": providerGuid,
		},
		"Security": map[string]interface{}{},
		"Task":     fmt.Sprintf("%d", task),
		"TimeCreated": map[string]interface{}{
			"SystemTime": eventTime.UTC().Format("2006-01-02T15:04:05.0000000Z"),
		},
		"Version":   fmt.Sprintf("%d", version),
		"_event_id": fmt.Sprintf("%d", eventID),
	}

	return system
}

func generateSecurityLogon(eventTime time.Time, sequence int) Event {
	account := systemAccounts[sequence%len(systemAccounts)]

	system := makeSystemBlock(eventTime, sequence,
		"Microsoft-Windows-Security-Auditing",
		"{54849625-5478-4994-A5BA-3E3B0328C30D}",
		"Security",
		4624, 0, 12544, 0, 2,
		"0x8020000000000000",
	)
	system["Security"] = map[string]interface{}{
		"UserID": "S-1-5-18",
	}

	eventData := map[string]interface{}{
		"SubjectUserSid":            "S-1-5-18",
		"SubjectUserName":           account,
		"SubjectDomainName":         "NT AUTHORITY",
		"SubjectLogonId":            "0x3e7",
		"TargetUserSid":             "S-1-5-18",
		"TargetUserName":            account,
		"TargetDomainName":          "NT AUTHORITY",
		"TargetLogonId":             "0x3e7",
		"LogonType":                 "5",
		"LogonProcessName":          "Advapi",
		"AuthenticationPackageName": "Negotiate",
		"WorkstationName":           "-",
		"LogonGuid":                 "{00000000-0000-0000-0000-000000000000}",
		"TransmittedServices":       "-",
		"LmPackageName":             "-",
		"KeyLength":                 "0",
		"ProcessId":                 "0x27c",
		"ProcessName":               "C:\\Windows\\System32\\services.exe",
		"IpAddress":                 "-",
		"IpPort":                    "-",
		"ImpersonationLevel":        "%%1833",
		"RestrictedAdminMode":       "-",
		"TargetOutboundUserName":    "-",
		"TargetOutboundDomainName":  "-",
		"VirtualAccount":            "%%1843",
		"TargetLinkedLogonId":       "0x0",
		"ElevatedToken":             "%%1842",
	}

	payload := map[string]interface{}{
		"EVENT": map[string]interface{}{
			"System":    system,
			"EventData": eventData,
		},
	}

	return Event{JsonPayload: payload, Timestamp: eventTime, EventType: "WEL"}
}

func generateSecurityLogoff(eventTime time.Time, sequence int) Event {
	account := systemAccounts[sequence%len(systemAccounts)]

	system := makeSystemBlock(eventTime, sequence,
		"Microsoft-Windows-Security-Auditing",
		"{54849625-5478-4994-A5BA-3E3B0328C30D}",
		"Security",
		4634, 0, 12545, 0, 0,
		"0x8020000000000000",
	)

	eventData := map[string]interface{}{
		"TargetUserSid":    "S-1-5-18",
		"TargetUserName":   account,
		"TargetDomainName": "NT AUTHORITY",
		"TargetLogonId":    "0x3e7",
		"LogonType":        "5",
	}

	payload := map[string]interface{}{
		"EVENT": map[string]interface{}{
			"System":    system,
			"EventData": eventData,
		},
	}

	return Event{JsonPayload: payload, Timestamp: eventTime, EventType: "WEL"}
}

func generateSecuritySpecialPrivileges(eventTime time.Time, sequence int) Event {
	account := systemAccounts[sequence%len(systemAccounts)]

	system := makeSystemBlock(eventTime, sequence,
		"Microsoft-Windows-Security-Auditing",
		"{54849625-5478-4994-A5BA-3E3B0328C30D}",
		"Security",
		4672, 0, 12548, 0, 0,
		"0x8020000000000000",
	)

	eventData := map[string]interface{}{
		"SubjectUserSid":    "S-1-5-18",
		"SubjectUserName":   account,
		"SubjectDomainName": "NT AUTHORITY",
		"SubjectLogonId":    "0x3e7",
		"PrivilegeList":     "SeAssignPrimaryTokenPrivilege SeTcbPrivilege SeSecurityPrivilege SeTakeOwnershipPrivilege SeLoadDriverPrivilege SeBackupPrivilege SeRestorePrivilege SeDebugPrivilege SeAuditPrivilege SeSystemEnvironmentPrivilege SeImpersonatePrivilege SeDelegateSessionUserImpersonatePrivilege",
	}

	payload := map[string]interface{}{
		"EVENT": map[string]interface{}{
			"System":    system,
			"EventData": eventData,
		},
	}

	return Event{JsonPayload: payload, Timestamp: eventTime, EventType: "WEL"}
}

func generateSystemSCM(eventTime time.Time, sequence int) Event {
	svc := services[sequence%len(services)]
	state := "running"
	if sequence%3 == 0 {
		state = "stopped"
	}

	system := makeSystemBlock(eventTime, sequence,
		"Service Control Manager",
		"{555908D1-A6D7-4695-8E1E-26931D2012F4}",
		"System",
		7036, 4, 0, 0, 0,
		"0x8080000000000000",
	)
	// Add EventSourceName for SCM
	system["Provider"].(map[string]interface{})["EventSourceName"] = "Service Control Manager"

	eventData := map[string]interface{}{
		"param1": svc.DisplayName,
		"param2": state,
		"Binary": svc.Name,
	}

	payload := map[string]interface{}{
		"EVENT": map[string]interface{}{
			"System":    system,
			"EventData": eventData,
		},
	}

	return Event{JsonPayload: payload, Timestamp: eventTime, EventType: "WEL"}
}

func generateSystemTimeSync(eventTime time.Time, sequence int) Event {
	system := makeSystemBlock(eventTime, sequence,
		"Microsoft-Windows-Time-Service",
		"{06EDCFEB-0FD0-4E53-ACCA-A6F8BBF81BCB}",
		"System",
		37, 4, 0, 0, 0,
		"0x8000000000000000",
	)
	system["Security"] = map[string]interface{}{
		"UserID": "S-1-5-19",
	}

	eventData := map[string]interface{}{
		"TimeSource": "time.windows.com,0x8",
	}

	payload := map[string]interface{}{
		"EVENT": map[string]interface{}{
			"System":    system,
			"EventData": eventData,
		},
	}

	return Event{JsonPayload: payload, Timestamp: eventTime, EventType: "WEL"}
}

func generateApplicationInfo(eventTime time.Time, sequence int) Event {
	apps := []struct {
		Name string
		Guid string
	}{
		{"Microsoft-Windows-User Profiles Service", "{89B1E9F0-5AFF-44A6-9B25-0A246C0BF2F0}"},
		{"Microsoft-Windows-Winlogon", "{DBE9B383-7CF3-4331-91CC-A3CB16A3B538}"},
		{"Microsoft-Windows-WindowsUpdateClient", "{945A8954-C147-4ACD-923F-40C45405A658}"},
		{"VSS", "{9138500E-3648-4EDB-AA4C-859E9F7B7C38}"},
		{"Desktop Window Manager", "{31F60101-3703-48EA-8143-451F8DE779D2}"},
	}
	app := apps[sequence%len(apps)]

	system := makeSystemBlock(eventTime, sequence,
		app.Name,
		app.Guid,
		"Application",
		1000, 4, 0, 0, 0,
		"0x8000000000000000",
	)

	eventData := map[string]interface{}{
		"Data": "Background task completed successfully",
	}

	payload := map[string]interface{}{
		"EVENT": map[string]interface{}{
			"System":    system,
			"EventData": eventData,
		},
	}

	return Event{JsonPayload: payload, Timestamp: eventTime, EventType: "WEL"}
}

func generateGUID(seed int) string {
	r := rand.New(rand.NewSource(int64(seed)))
	return fmt.Sprintf("{%08x-%04x-%04x-%04x-%012x}",
		r.Uint32(),
		r.Uint32()&0xFFFF,
		r.Uint32()&0xFFFF,
		r.Uint32()&0xFFFF,
		r.Uint64()&0xFFFFFFFFFFFF,
	)
}
