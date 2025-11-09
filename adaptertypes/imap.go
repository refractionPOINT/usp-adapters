package adaptertypes

// ImapConfig defines the configuration for the IMAP email adapter
type ImapConfig struct {
	ClientOptions          ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Server                 string        `json:"server" yaml:"server" description:"IMAP server address with port" category:"source" example:"imap.gmail.com:993"`
	UserName               string        `json:"username" yaml:"username" description:"IMAP username/email" category:"auth"`
	Password               string        `json:"password" yaml:"password" description:"IMAP password or app password" category:"auth" sensitive:"true"`
	InboxName              string        `json:"inbox_name" yaml:"inbox_name" description:"IMAP folder/inbox name to monitor" category:"source" default:"INBOX"`
	IsInsecure             bool          `json:"is_insecure" yaml:"is_insecure" description:"Skip TLS certificate verification" category:"behavior" default:"false" llmguidance:"Only enable for testing. Keep false for production"`
	FromZero               bool          `json:"from_zero" yaml:"from_zero" description:"Start from first email in inbox" category:"behavior" default:"false" llmguidance:"If true, process all emails. If false, only process new emails"`
	IncludeAttachments     bool          `json:"include_attachments" yaml:"include_attachments" description:"Extract and include email attachments" category:"parsing" default:"false"`
	MaxBodySize            int           `json:"max_body_size" yaml:"max_body_size" description:"Maximum email body size in bytes" category:"parsing" default:"1048576" llmguidance:"Default 1MB. Increase for large emails"`
	AttachmentIngestKey    string        `json:"attachment_ingest_key" yaml:"attachment_ingest_key" description:"LimaCharlie ingestion key for uploading attachments" category:"client"`
	AttachmentRetentionDays int          `json:"attachment_retention_days" yaml:"attachment_retention_days" description:"Days to retain uploaded attachments" category:"client" default:"30"`
}
