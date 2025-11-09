package conf

import (
	"github.com/refractionPOINT/usp-adapters/adaptertypes"
)

type GeneralConfigs struct {
	Healthcheck int `json:"healthcheck" yaml:"healthcheck"`

	Syslog            adaptertypes.SyslogConfig            `json:"syslog" yaml:"syslog"`
	PubSub            adaptertypes.PubSubConfig            `json:"pubsub" yaml:"pubsub"`
	S3                adaptertypes.S3Config                `json:"s3" yaml:"s3"`
	Stdin             adaptertypes.StdinConfig             `json:"stdin" yaml:"stdin"`
	OnePassword       adaptertypes.OnePasswordConfig       `json:"1password" yaml:"1password"`
	Bitwarden         adaptertypes.BitwardenConfig         `json:"bitwarden" yaml:"bitwarden"`
	ITGlue            adaptertypes.ITGlueConfig            `json:"itglue" yaml:"itglue"`
	Sophos            adaptertypes.SophosConfig            `json:"sophos" yaml:"sophos"`
	EntraID           adaptertypes.EntraIDConfig           `json:"entraid" yaml:"entraid"`
	Defender          adaptertypes.DefenderConfig          `json:"defender" yaml:"defender"`
	Cato              adaptertypes.CatoConfig              `json:"cato" yaml:"cato"`
	Cylance           adaptertypes.CylanceConfig           `json:"cylance" yaml:"cylance"`
	Okta              adaptertypes.OktaConfig              `json:"okta" yaml:"okta"`
	Office365         adaptertypes.Office365Config         `json:"office365" yaml:"office365"`
	Wel               adaptertypes.WELConfig               `json:"wel" yaml:"wel"`
	MacUnifiedLogging adaptertypes.MacUnifiedLoggingConfig `json:"mac_unified_logging" yaml:"mac_unified_logging"`
	AzureEventHub     adaptertypes.EventHubConfig          `json:"azure_event_hub" yaml:"azure_event_hub"`
	Duo               adaptertypes.DuoConfig               `json:"duo" yaml:"duo"`
	Gcs               adaptertypes.GCSConfig               `json:"gcs" yaml:"gcs"`
	Slack             adaptertypes.SlackConfig             `json:"slack" yaml:"slack"`
	Sqs               adaptertypes.SQSConfig               `json:"sqs" yaml:"sqs"`
	SqsFiles          adaptertypes.SQSFilesConfig          `json:"sqs-files" yaml:"sqs-files"`
	Simulator         adaptertypes.SimulatorConfig         `json:"simulator" yaml:"simulator"`
	File              adaptertypes.FileConfig              `json:"file" yaml:"file"`
	Evtx              adaptertypes.EVTXConfig              `json:"evtx" yaml:"evtx"`
	K8sPods           adaptertypes.K8sPodsConfig           `json:"k8s_pods" yaml:"k8s_pods"`
	BigQuery          adaptertypes.BigQueryConfig          `json:"bigquery" yaml:"bigquery"`
	Imap              adaptertypes.ImapConfig              `json:"imap" yaml:"imap"`
	HubSpot           adaptertypes.HubSpotConfig           `json:"hubspot" yaml:"hubspot"`
	FalconCloud       adaptertypes.FalconCloudConfig       `json:"falconcloud" yaml:"falconcloud"`
	Mimecast          adaptertypes.MimecastConfig          `json:"mimecast" yaml:"mimecast"`
	MsGraph           adaptertypes.MsGraphConfig           `json:"ms_graph" yaml:"ms_graph"`
	Zendesk           adaptertypes.ZendeskConfig           `json:"zendesk" yaml:"zendesk"`
	PandaDoc          adaptertypes.PandaDocConfig          `json:"pandadoc" yaml:"pandadoc"`
	ProofpointTap     adaptertypes.ProofpointTapConfig     `json:"proofpoint_tap" yaml:"proofpoint_tap"`
	Box               adaptertypes.BoxConfig               `json:"box" yaml:"box"`
	Sublime           adaptertypes.SublimeConfig           `json:"sublime" yaml:"sublime"`
	SentinelOne       adaptertypes.SentinelOneConfig       `json:"sentinel_one" yaml:"sentinel_one"`
	TrendMicro        adaptertypes.TrendMicroConfig        `json:"trendmicro" yaml:"trendmicro"`
	Wiz               adaptertypes.WizConfig               `json:"wiz" yaml:"wiz"`
}
