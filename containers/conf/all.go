package conf

import (
	usp_bigquery "github.com/refractionPOINT/usp-adapters/bigquery"

	"github.com/refractionPOINT/usp-adapters/1password"
	"github.com/refractionPOINT/usp-adapters/azure_event_hub"
	"github.com/refractionPOINT/usp-adapters/cato"
	"github.com/refractionPOINT/usp-adapters/defender"
	"github.com/refractionPOINT/usp-adapters/duo"
	"github.com/refractionPOINT/usp-adapters/entraid"
	"github.com/refractionPOINT/usp-adapters/evtx"
	"github.com/refractionPOINT/usp-adapters/falconcloud"
	"github.com/refractionPOINT/usp-adapters/file"
	"github.com/refractionPOINT/usp-adapters/gcs"
	"github.com/refractionPOINT/usp-adapters/hubspot"
	"github.com/refractionPOINT/usp-adapters/imap"
	"github.com/refractionPOINT/usp-adapters/itglue"
	"github.com/refractionPOINT/usp-adapters/k8s_pods"
	"github.com/refractionPOINT/usp-adapters/mac_unified_logging"
	"github.com/refractionPOINT/usp-adapters/mimecast"
	"github.com/refractionPOINT/usp-adapters/ms_graph"
	"github.com/refractionPOINT/usp-adapters/o365"
	"github.com/refractionPOINT/usp-adapters/okta"
	"github.com/refractionPOINT/usp-adapters/pandadoc"
	"github.com/refractionPOINT/usp-adapters/pubsub"
	"github.com/refractionPOINT/usp-adapters/s3"
	"github.com/refractionPOINT/usp-adapters/sentinelone"
	"github.com/refractionPOINT/usp-adapters/simulator"
	"github.com/refractionPOINT/usp-adapters/slack"
	"github.com/refractionPOINT/usp-adapters/sophos"
	"github.com/refractionPOINT/usp-adapters/sqs"
	"github.com/refractionPOINT/usp-adapters/sqs-files"
	"github.com/refractionPOINT/usp-adapters/stdin"
	"github.com/refractionPOINT/usp-adapters/syslog"
	"github.com/refractionPOINT/usp-adapters/wel"
	"github.com/refractionPOINT/usp-adapters/zendesk"
)

type GeneralConfigs struct {
	Healthcheck int `json:"healthcheck" yaml:"healthcheck"`

	Syslog            usp_syslog.SyslogConfig                         `json:"syslog" yaml:"syslog"`
	PubSub            usp_pubsub.PubSubConfig                         `json:"pubsub" yaml:"pubsub"`
	S3                usp_s3.S3Config                                 `json:"s3" yaml:"s3"`
	Stdin             usp_stdin.StdinConfig                           `json:"stdin" yaml:"stdin"`
	OnePassword       usp_1password.OnePasswordConfig                 `json:"1password" yaml:"1password"`
	ITGlue            usp_itglue.ITGlueConfig                         `json:"itglue" yaml:"itglue"`
	Sophos            usp_sophos.SophosConfig                         `json:"sophos" yaml:"sophos"`
	EntraID           usp_entraid.EntraIDConfig                       `json:"entraid" yaml:"entraid"`
	Defender          usp_defender.DefenderConfig                     `json:"defender" yaml:"defender"`
	Cato              usp_cato.CatoConfig                             `json:"cato" yaml:"cato"`
	Okta              usp_okta.OktaConfig                             `json:"okta" yaml:"okta"`
	Office365         usp_o365.Office365Config                        `json:"office365" yaml:"office365"`
	Wel               usp_wel.WELConfig                               `json:"wel" yaml:"wel"`
	MacUnifiedLogging usp_mac_unified_logging.MacUnifiedLoggingConfig `json:"mac_unified_logging" yaml:"mac_unified_logging"`
	AzureEventHub     usp_azure_event_hub.EventHubConfig              `json:"azure_event_hub" yaml:"azure_event_hub"`
	Duo               usp_duo.DuoConfig                               `json:"duo" yaml:"duo"`
	Gcs               usp_gcs.GCSConfig                               `json:"gcs" yaml:"gcs"`
	Slack             usp_slack.SlackConfig                           `json:"slack" yaml:"slack"`
	Sqs               usp_sqs.SQSConfig                               `json:"sqs" yaml:"sqs"`
	SqsFiles          usp_sqs_files.SQSFilesConfig                    `json:"sqs-files" yaml:"sqs-files"`
	Simulator         usp_simulator.SimulatorConfig                   `json:"simulator" yaml:"simulator"`
	File              usp_file.FileConfig                             `json:"file" yaml:"file"`
	Evtx              usp_evtx.EVTXConfig                             `json:"evtx" yaml:"evtx"`
	K8sPods           usp_k8s_pods.K8sPodsConfig                      `json:"k8s_pods" yaml:"k8s_pods"`
	BigQuery          usp_bigquery.BigQueryConfig                     `json:"bigquery" yaml:"bigquery"`
	Imap              usp_imap.ImapConfig                             `json:"imap" yaml:"imap"`
	HubSpot           usp_hubspot.HubSpotConfig                       `json:"hubspot" yaml:"hubspot"`
	FalconCloud       usp_falconcloud.FalconCloudConfig               `json:"falconcloud" yaml:"falconcloud"`
	Mimecast          usp_mimecast.MimecastConfig                     `json:"mimecast" yaml:"mimecast"`
	MsGraph           usp_ms_graph.MsGraphConfig                      `json:"ms_graph" yaml:"ms_graph"`
	Zendesk           usp_zendesk.ZendeskConfig                       `json:"zendesk" yaml:"zendesk"`
	PandaDoc          usp_pandadoc.PandaDocConfig                     `json:"pandadoc" yaml:"pandadoc"`
	SentinelOne       usp_sentinelone.SentinelOneConfig               `json:"sentinel_one" yaml:"sentinel_one"`
}
