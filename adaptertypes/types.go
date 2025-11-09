package adaptertypes

// AdapterConfigs is the central registry of all adapter configuration types.
// This struct is used for reflection-based schema generation.
type AdapterConfigs struct {
	// Cloud Storage
	S3  S3Config  `json:"s3" yaml:"s3" adapterName:"s3" adapterCategory:"cloud_storage" description:"Amazon S3 bucket log ingestion"`
	GCS GCSConfig `json:"gcs" yaml:"gcs" adapterName:"gcs" adapterCategory:"cloud_storage" description:"Google Cloud Storage bucket log ingestion"`

	// File-Based
	File     FileConfig      `json:"file" yaml:"file" adapterName:"file" adapterCategory:"file_based" description:"Local file and directory monitoring"`
	EVTX     EVTXConfig      `json:"evtx" yaml:"evtx" adapterName:"evtx" adapterCategory:"file_based" description:"Windows Event Log (.evtx) file parser"`
	WEL      WELConfig       `json:"wel" yaml:"wel" adapterName:"wel" adapterCategory:"file_based" description:"Live Windows Event Log monitoring"`
	Syslog   SyslogConfig    `json:"syslog" yaml:"syslog" adapterName:"syslog" adapterCategory:"file_based" description:"Syslog server (UDP/TCP/TLS)"`
	Stdin    StdinConfig     `json:"stdin" yaml:"stdin" adapterName:"stdin" adapterCategory:"file_based" description:"Standard input stream ingestion"`
	Simulator SimulatorConfig `json:"simulator" yaml:"simulator" adapterName:"simulator" adapterCategory:"file_based" description:"Event replay simulator for testing"`

	// Authentication & IAM
	OnePassword OnePasswordConfig `json:"1password" yaml:"1password" adapterName:"1password" adapterCategory:"authentication" description:"1Password event logs"`
	Bitwarden   BitwardenConfig   `json:"bitwarden" yaml:"bitwarden" adapterName:"bitwarden" adapterCategory:"authentication" description:"Bitwarden event logs"`
	Duo         DuoConfig         `json:"duo" yaml:"duo" adapterName:"duo" adapterCategory:"authentication" description:"Duo Security authentication logs"`
	Okta        OktaConfig        `json:"okta" yaml:"okta" adapterName:"okta" adapterCategory:"authentication" description:"Okta system logs"`
	EntraID     EntraIDConfig     `json:"entraid" yaml:"entraid" adapterName:"entraid" adapterCategory:"authentication" description:"Microsoft Entra ID (Azure AD) audit logs"`

	// Security Products
	Defender     DefenderConfig      `json:"defender" yaml:"defender" adapterName:"defender" adapterCategory:"security_products" description:"Microsoft Defender for Endpoint"`
	SentinelOne  SentinelOneConfig   `json:"sentinel_one" yaml:"sentinel_one" adapterName:"sentinel_one" adapterCategory:"security_products" description:"SentinelOne EDR platform"`
	FalconCloud  FalconCloudConfig   `json:"falconcloud" yaml:"falconcloud" adapterName:"falconcloud" adapterCategory:"security_products" description:"CrowdStrike Falcon"`
	Cylance      CylanceConfig       `json:"cylance" yaml:"cylance" adapterName:"cylance" adapterCategory:"security_products" description:"BlackBerry Cylance"`
	Sophos       SophosConfig        `json:"sophos" yaml:"sophos" adapterName:"sophos" adapterCategory:"security_products" description:"Sophos Central"`
	TrendMicro   TrendMicroConfig    `json:"trendmicro" yaml:"trendmicro" adapterName:"trendmicro" adapterCategory:"security_products" description:"Trend Micro security platform"`
	Wiz          WizConfig           `json:"wiz" yaml:"wiz" adapterName:"wiz" adapterCategory:"security_products" description:"Wiz cloud security platform"`
	ProofpointTap ProofpointTapConfig `json:"proofpoint_tap" yaml:"proofpoint_tap" adapterName:"proofpoint_tap" adapterCategory:"security_products" description:"Proofpoint Targeted Attack Protection"`
	Sublime      SublimeConfig       `json:"sublime" yaml:"sublime" adapterName:"sublime" adapterCategory:"security_products" description:"Sublime Security email security"`

	// Cloud Platforms
	PubSub        PubSubConfig      `json:"pubsub" yaml:"pubsub" adapterName:"pubsub" adapterCategory:"cloud_platforms" description:"Google Cloud Pub/Sub"`
	BigQuery      BigQueryConfig    `json:"bigquery" yaml:"bigquery" adapterName:"bigquery" adapterCategory:"cloud_platforms" description:"Google BigQuery data warehouse"`
	AzureEventHub EventHubConfig    `json:"azure_event_hub" yaml:"azure_event_hub" adapterName:"azure_event_hub" adapterCategory:"cloud_platforms" description:"Azure Event Hubs"`
	SQS           SQSConfig         `json:"sqs" yaml:"sqs" adapterName:"sqs" adapterCategory:"cloud_platforms" description:"AWS Simple Queue Service"`
	SQSFiles      SQSFilesConfig    `json:"sqs-files" yaml:"sqs-files" adapterName:"sqs-files" adapterCategory:"cloud_platforms" description:"AWS SQS with S3 file downloads"`
	K8sPods       K8sPodsConfig     `json:"k8s_pods" yaml:"k8s_pods" adapterName:"k8s_pods" adapterCategory:"cloud_platforms" description:"Kubernetes pod logs"`
	MacUnifiedLogging MacUnifiedLoggingConfig `json:"mac_unified_logging" yaml:"mac_unified_logging" adapterName:"mac_unified_logging" adapterCategory:"cloud_platforms" description:"macOS Unified Logging System"`

	// Communication & Collaboration
	Slack   SlackConfig   `json:"slack" yaml:"slack" adapterName:"slack" adapterCategory:"communication" description:"Slack audit logs"`
	Office365 Office365Config `json:"office365" yaml:"office365" adapterName:"office365" adapterCategory:"communication" description:"Office 365 Management Activity API"`
	MsGraph MsGraphConfig `json:"ms_graph" yaml:"ms_graph" adapterName:"ms_graph" adapterCategory:"communication" description:"Microsoft Graph API"`
	Mimecast MimecastConfig `json:"mimecast" yaml:"mimecast" adapterName:"mimecast" adapterCategory:"communication" description:"Mimecast email security"`
	IMAP     ImapConfig     `json:"imap" yaml:"imap" adapterName:"imap" adapterCategory:"communication" description:"IMAP email monitoring"`

	// Business Tools
	HubSpot  HubSpotConfig  `json:"hubspot" yaml:"hubspot" adapterName:"hubspot" adapterCategory:"business_tools" description:"HubSpot CRM"`
	ITGlue   ITGlueConfig   `json:"itglue" yaml:"itglue" adapterName:"itglue" adapterCategory:"business_tools" description:"IT Glue documentation platform"`
	Zendesk  ZendeskConfig  `json:"zendesk" yaml:"zendesk" adapterName:"zendesk" adapterCategory:"business_tools" description:"Zendesk customer support platform"`
	PandaDoc PandaDocConfig `json:"pandadoc" yaml:"pandadoc" adapterName:"pandadoc" adapterCategory:"business_tools" description:"PandaDoc document management"`
	Box      BoxConfig      `json:"box" yaml:"box" adapterName:"box" adapterCategory:"business_tools" description:"Box cloud storage and collaboration"`

	// Network
	Cato CatoConfig `json:"cato" yaml:"cato" adapterName:"cato" adapterCategory:"network" description:"Cato Networks SASE platform"`
}

// GetAdapterRegistry returns a new instance of the adapter registry
func GetAdapterRegistry() *AdapterConfigs {
	return &AdapterConfigs{}
}
