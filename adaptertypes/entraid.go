package adaptertypes

// EntraIDConfig defines the configuration for the Microsoft Entra ID (Azure AD) adapter
type EntraIDConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	TenantID      string        `json:"tenant_id" yaml:"tenant_id" description:"Azure AD tenant ID" category:"auth" example:"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`
	ClientID      string        `json:"client_id" yaml:"client_id" description:"Azure AD application (client) ID" category:"auth" example:"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`
	ClientSecret  string        `json:"client_secret" yaml:"client_secret" description:"Azure AD application client secret" category:"auth" sensitive:"true"`
}
