package usp_unifi_syslog

import (
	"strings"
	"testing"

	"github.com/refractionPOINT/usp-adapters/utils"
	"github.com/stretchr/testify/assert"
)

func TestParseCEF_AdminAccess(t *testing.T) {
	line := `CEF:0|Ubiquiti|UniFi Network|9.3.33|544|Admin Accessed UniFi Network|1|UNIFIcategory=System UNIFIsubCategory=Admin UNIFIhost=Office UDM Pro UNIFIaccessMethod=web UNIFIadmin=Craig src=105.5.138.59 msg=Craig accessed UniFi Network using the web. Source IP: 105.5.138.59`

	result, ok := parseCEF(line)
	assert.True(t, ok)
	assert.Equal(t, "0", result["cef_version"])
	assert.Equal(t, "Ubiquiti", result["device_vendor"])
	assert.Equal(t, "UniFi Network", result["device_product"])
	assert.Equal(t, "9.3.33", result["device_version"])
	assert.Equal(t, "544", result["device_event_class_id"])
	assert.Equal(t, "Admin Accessed UniFi Network", result["name"])
	assert.Equal(t, "1", result["severity"])

	ext := result["extension"].(utils.Dict)
	assert.Equal(t, "System", ext["UNIFIcategory"])
	assert.Equal(t, "Admin", ext["UNIFIsubCategory"])
	assert.Equal(t, "Office UDM Pro", ext["UNIFIhost"])
	assert.Equal(t, "web", ext["UNIFIaccessMethod"])
	assert.Equal(t, "Craig", ext["UNIFIadmin"])
	assert.Equal(t, "105.5.138.59", ext["src"])
	assert.Equal(t, "Craig accessed UniFi Network using the web. Source IP: 105.5.138.59", ext["msg"])
}

func TestParseCEF_WiFiClientDisconnected(t *testing.T) {
	line := `CEF:0|Ubiquiti|UniFi Network|9.3.33|401|WiFi Client Disconnected|2|UNIFIcategory=Monitoring UNIFIsubCategory=WiFi UNIFIhost=Office UDM Pro UNIFIlastConnectedToDeviceName=Lobby AP UNIFIlastConnectedToDeviceIp=192.168.100.5 UNIFIlastConnectedToDeviceMac=d8:b3:70:fb:fc:dd UNIFIlastConnectedToDeviceModel=U7-Pro UNIFIlastConnectedToDeviceVersion=8.0.9 UNIFIclientAlias=Apple Watch 0d:87 UNIFIclientHostname=Craig Watch UNIFIclientIp=192.168.10.178 UNIFIclientMac=0a:be:db:c8:0d:81 UNIFIwifiChannel=153 UNIFIwifiChannelWidth=20 UNIFIwifiName=Employee WiFi UNIFIwifiBand=na UNIFIwifiAirtimeUtilization=14 UNIFIwifiInterference=9 UNIFIlastConnectedToWiFiRssi=-77 UNIFIduration=6m 22s UNIFIusageDown=11.78 KB UNIFIusageUp=4.46 KB UNIFInetworkName=Employee Network UNIFInetworkSubnet=192.168.10.0/24 UNIFInetworkVlan=10 msg=Apple Watch 0d:87 disconnected from Employee WiFi. Time Connected: 6m 22s. Data Used: 4.46 KB (up) / 11.78 KB (down). Last Connected To: Lobby AP at -77 dBm.`

	result, ok := parseCEF(line)
	assert.True(t, ok)
	assert.Equal(t, "Ubiquiti", result["device_vendor"])
	assert.Equal(t, "401", result["device_event_class_id"])
	assert.Equal(t, "WiFi Client Disconnected", result["name"])

	ext := result["extension"].(utils.Dict)
	assert.Equal(t, "Monitoring", ext["UNIFIcategory"])
	assert.Equal(t, "WiFi", ext["UNIFIsubCategory"])
	assert.Equal(t, "Office UDM Pro", ext["UNIFIhost"])
	assert.Equal(t, "Lobby AP", ext["UNIFIlastConnectedToDeviceName"])
	assert.Equal(t, "192.168.100.5", ext["UNIFIlastConnectedToDeviceIp"])
	assert.Equal(t, "d8:b3:70:fb:fc:dd", ext["UNIFIlastConnectedToDeviceMac"])
	assert.Equal(t, "U7-Pro", ext["UNIFIlastConnectedToDeviceModel"])
	assert.Equal(t, "Apple Watch 0d:87", ext["UNIFIclientAlias"])
	assert.Equal(t, "Craig Watch", ext["UNIFIclientHostname"])
	assert.Equal(t, "192.168.10.178", ext["UNIFIclientIp"])
	assert.Equal(t, "153", ext["UNIFIwifiChannel"])
	assert.Equal(t, "Employee WiFi", ext["UNIFIwifiName"])
	assert.Equal(t, "-77", ext["UNIFIlastConnectedToWiFiRssi"])
	assert.Equal(t, "6m 22s", ext["UNIFIduration"])
	assert.Equal(t, "11.78 KB", ext["UNIFIusageDown"])
	assert.Equal(t, "4.46 KB", ext["UNIFIusageUp"])
	assert.Equal(t, "Employee Network", ext["UNIFInetworkName"])
	assert.Equal(t, "192.168.10.0/24", ext["UNIFInetworkSubnet"])
	assert.Equal(t, "10", ext["UNIFInetworkVlan"])
	assert.Contains(t, ext["msg"], "Apple Watch 0d:87 disconnected from Employee WiFi")
}

func TestParseCEF_NotCEF(t *testing.T) {
	_, ok := parseCEF("just a regular syslog line")
	assert.False(t, ok)
}

func TestParseCEF_IncompleteCEF(t *testing.T) {
	_, ok := parseCEF("CEF:0|Ubiquiti|UniFi Network|9.3.33|544|Admin Accessed")
	assert.False(t, ok)
}

func TestParseCEF_EmptyExtension(t *testing.T) {
	line := `CEF:0|Ubiquiti|UniFi Network|9.3.33|100|Test Event|1|`
	result, ok := parseCEF(line)
	assert.True(t, ok)
	assert.Equal(t, "Test Event", result["name"])
	_, hasExt := result["extension"]
	assert.False(t, hasExt)
}

func TestParseCEFExtension_SimpleKeyValues(t *testing.T) {
	ext := parseCEFExtension("src=1.2.3.4 dst=5.6.7.8")
	assert.Equal(t, "1.2.3.4", ext["src"])
	assert.Equal(t, "5.6.7.8", ext["dst"])
}

func TestParseCEFExtension_ValuesWithSpaces(t *testing.T) {
	ext := parseCEFExtension("UNIFIhost=Office UDM Pro src=1.2.3.4")
	assert.Equal(t, "Office UDM Pro", ext["UNIFIhost"])
	assert.Equal(t, "1.2.3.4", ext["src"])
}

func TestParseCEFExtension_MsgAtEnd(t *testing.T) {
	ext := parseCEFExtension("src=1.2.3.4 msg=This is a long message with spaces and special=chars")
	assert.Equal(t, "1.2.3.4", ext["src"])
	assert.Equal(t, "This is a long message with spaces and special=chars", ext["msg"])
}

func TestParseCEFExtension_Empty(t *testing.T) {
	ext := parseCEFExtension("")
	assert.Nil(t, ext)
}

func TestParseCEF_SyslogWrapped(t *testing.T) {
	// Real UniFi syslog has a BSD syslog header before CEF payload.
	line := `Feb  5 22:19:15 UDM-Pro-Max CEF:0|Ubiquiti|UniFi Network|9.3.33|544|Admin Accessed UniFi Network|1|UNIFIcategory=System src=1.2.3.4 msg=Test`

	// Find CEF: within the line, same as handleLine does.
	cefLine := line
	if idx := strings.Index(line, "CEF:"); idx > 0 {
		cefLine = line[idx:]
	}

	result, ok := parseCEF(cefLine)
	assert.True(t, ok)
	assert.Equal(t, "Ubiquiti", result["device_vendor"])
	assert.Equal(t, "Admin Accessed UniFi Network", result["name"])
	ext := result["extension"].(utils.Dict)
	assert.Equal(t, "System", ext["UNIFIcategory"])
	assert.Equal(t, "1.2.3.4", ext["src"])
}

func TestParseSyslog_DoubledHostname(t *testing.T) {
	result := parseSyslog("Feb  5 22:23:49 UDM-Pro-Max UDM-Pro-Max mcad[4857]: udapi_cache.udapi_cache_set_global_update_interval(): Bumping global update interval :: interval=30000msec->20000msec")
	assert.Equal(t, "Feb  5 22:23:49", result["timestamp"])
	assert.Equal(t, "UDM-Pro-Max", result["hostname"])
	assert.Equal(t, "mcad", result["process"])
	assert.Equal(t, "4857", result["pid"])
	assert.Contains(t, result["message"], "Bumping global update interval")
}

func TestParseSyslog_SingleHostname(t *testing.T) {
	result := parseSyslog("Feb  5 22:19:15 UDM-Pro kernel: [UFW BLOCK] IN=br0 OUT= SRC=10.0.0.99")
	assert.Equal(t, "Feb  5 22:19:15", result["timestamp"])
	assert.Equal(t, "UDM-Pro", result["hostname"])
	assert.Equal(t, "kernel", result["process"])
	assert.Nil(t, result["pid"])
	assert.Contains(t, result["message"], "[UFW BLOCK]")
}

func TestParseSyslog_WithPID(t *testing.T) {
	result := parseSyslog("Feb  5 22:19:20 UDM-Pro-Max UDM-Pro-Max ubios-udapi-server[2160]: process: Process' stime is unknown")
	assert.Equal(t, "UDM-Pro-Max", result["hostname"])
	assert.Equal(t, "ubios-udapi-server", result["process"])
	assert.Equal(t, "2160", result["pid"])
}

func TestParseSyslog_Earlyoom(t *testing.T) {
	result := parseSyslog("Feb  5 22:19:15 UDM-Pro-Max UDM-Pro-Max earlyoom[1588]: mem avail:  3207 of  7972 MiB (40.24%), swap free: 6375 of 7167 MiB (88.95%)")
	assert.Equal(t, "UDM-Pro-Max", result["hostname"])
	assert.Equal(t, "earlyoom", result["process"])
	assert.Equal(t, "1588", result["pid"])
	assert.Contains(t, result["message"], "mem avail")
}

func TestParseSyslog_EmbeddedJSON(t *testing.T) {
	result := parseSyslog(`Feb  5 22:35:29 UDM-Pro-Max UDM-Pro-Max coredns[58367]: {"timestamp":"2026-02-05T22:35:29-06:00","type":"dnsAdBlock","domain":"distillery.wistia.com","ip":"192.168.10.31"}`)
	assert.Equal(t, "UDM-Pro-Max", result["hostname"])
	assert.Equal(t, "coredns", result["process"])
	assert.Equal(t, "58367", result["pid"])
	// Message should be a parsed map, not a string.
	msgMap, ok := result["message"].(map[string]interface{})
	assert.True(t, ok, "message should be parsed JSON, got %T", result["message"])
	assert.Equal(t, "dnsAdBlock", msgMap["type"])
	assert.Equal(t, "distillery.wistia.com", msgMap["domain"])
	assert.Equal(t, "192.168.10.31", msgMap["ip"])
}

func TestParseSyslog_NonJSONMessage(t *testing.T) {
	result := parseSyslog("Feb  5 22:23:49 UDM-Pro kernel: just a normal message")
	msg, ok := result["message"].(string)
	assert.True(t, ok, "message should be string, got %T", result["message"])
	assert.Equal(t, "just a normal message", msg)
}

func TestParseSyslog_NoMatch(t *testing.T) {
	result := parseSyslog("some random text")
	assert.Equal(t, "some random text", result["raw"])
	assert.Nil(t, result["hostname"])
}

func TestParseCEF_SyslogWithPriorityAndHost(t *testing.T) {
	// Syslog with priority header + timestamp + hostname.
	line := `<134>Feb  5 22:19:15 UDM-Pro-Max CEF:0|Ubiquiti|UniFi Network|9.3.33|401|WiFi Client Disconnected|2|UNIFIcategory=Monitoring msg=Client disconnected`

	// Strip priority.
	if idx := strings.Index(line, ">"); idx != -1 && idx < 10 {
		line = line[idx+1:]
	}

	// Find CEF:.
	cefLine := line
	if idx := strings.Index(line, "CEF:"); idx > 0 {
		cefLine = line[idx:]
	}

	result, ok := parseCEF(cefLine)
	assert.True(t, ok)
	assert.Equal(t, "WiFi Client Disconnected", result["name"])
	ext := result["extension"].(utils.Dict)
	assert.Equal(t, "Monitoring", ext["UNIFIcategory"])
}
