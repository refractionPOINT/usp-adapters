package adaptertypes

type SyslogConfig struct {
	ClientOptions     ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Port              uint16        `json:"port" yaml:"port" description:"Port number to listen on for syslog messages" category:"source" example:"514" llmguidance:"Standard syslog port is 514 (requires root). Use 1514 or higher for non-root"`
	Interface         string        `json:"iface" yaml:"iface" description:"Network interface to bind to" category:"source" example:"0.0.0.0" llmguidance:"Use 0.0.0.0 to listen on all interfaces, or specify a specific IP address"`
	IsUDP             bool          `json:"is_udp,omitempty" yaml:"is_udp,omitempty" description:"Use UDP instead of TCP" category:"behavior" default:"false" llmguidance:"UDP is stateless and faster but less reliable. TCP is recommended for critical logs"`
	SslCertPath       string        `json:"ssl_cert" yaml:"ssl_cert" description:"Path to SSL/TLS certificate file" category:"auth" example:"/etc/ssl/certs/server.crt" llmguidance:"Required for TLS syslog. PEM format certificate file"`
	SslKeyPath        string        `json:"ssl_key" yaml:"ssl_key" description:"Path to SSL/TLS private key file" category:"auth" sensitive:"true" example:"/etc/ssl/private/server.key" llmguidance:"Required for TLS syslog. PEM format private key file"`
	MutualTlsCertPath string        `json:"mutual_tls_cert,omitempty" yaml:"mutual_tls_cert,omitempty" description:"Path to mutual TLS CA certificate for client authentication" category:"auth" example:"/etc/ssl/certs/ca.crt" llmguidance:"Optional. Enable mutual TLS by providing CA cert to verify client certificates"`
	WriteTimeoutSec   uint64        `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"600"`
}
