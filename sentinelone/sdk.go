package sentinelone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client represents a SentinelOne API client
type Client struct {
	baseURL    string
	apiToken   string
	httpClient *http.Client
}

// NewClient creates a new SentinelOne API client
func NewClient(baseURL, apiToken string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiToken:   apiToken,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Activity represents a SentinelOne activity
type Activity struct {
	AccountID            string      `json:"accountId"`
	AccountName          string      `json:"accountName"`
	ActivityType         int         `json:"activityType"`
	ActivityUUID         string      `json:"activityUuid"`
	AgentID              string      `json:"agentId"`
	AgentUpdatedVersion  string      `json:"agentUpdatedVersion"`
	Comments             string      `json:"comments"`
	CreatedAt            time.Time   `json:"createdAt"`
	Data                 interface{} `json:"data"`
	Description          string      `json:"description"`
	GroupID              string      `json:"groupId"`
	GroupName            string      `json:"groupName"`
	Hash                 string      `json:"hash"`
	ID                   string      `json:"id"`
	OSFamily             string      `json:"osFamily"`
	PrimaryDescription   string      `json:"primaryDescription"`
	SecondaryDescription string      `json:"secondaryDescription"`
	SiteID               string      `json:"siteId"`
	SiteName             string      `json:"siteName"`
	ThreatID             string      `json:"threatId"`
	UpdatedAt            time.Time   `json:"updatedAt"`
	UserID               string      `json:"userId"`
}

// PaginationData represents pagination information
type PaginationData struct {
	TotalItems int     `json:"totalItems"`
	NextCursor *string `json:"nextCursor"`
}

// ActivitiesResponse represents the response from the Get Activities endpoint
type ActivitiesResponse struct {
	Pagination PaginationData `json:"pagination"`
	Data       []Activity     `json:"data"`
	Errors     []string       `json:"errors"`
}

// GetActivitiesOptions represents the available options for filtering activities
type GetActivitiesOptions struct {
	AccountIDs       []string  `url:"accountIds,omitempty"`
	ActivityTypes    []int     `url:"activitytypes,omitempty"`
	ActivityUUIDs    []string  `url:"activityuuids,omitempty"`
	AgentIDs         []string  `url:"agentIds,omitempty"`
	AlertIDs         []string  `url:"alertIds,omitempty"`
	CountOnly        *bool     `url:"countonly,omitempty"`
	CreatedAtBetween string    `url:"createdat__between,omitempty"`
	CreatedAtGT      time.Time `url:"createdat__gt,omitempty"`
	CreatedAtGTE     time.Time `url:"createdat__gte,omitempty"`
	CreatedAtLT      time.Time `url:"createdat__lt,omitempty"`
	CreatedAtLTE     time.Time `url:"createdat__lte,omitempty"`
	Cursor           string    `url:"cursor,omitempty"`
	GroupIDs         []string  `url:"groupids,omitempty"`
	IDs              []string  `url:"ids,omitempty"`
	IncludeHidden    *bool     `url:"includehidden,omitempty"`
	Limit            int       `url:"limit,omitempty"`
	RuleIDs          []string  `url:"ruleids,omitempty"`
	SiteIDs          []string  `url:"siteids,omitempty"`
	Skip             int       `url:"skip,omitempty"`
	SkipCount        *bool     `url:"skipcount,omitempty"`
	SortBy           string    `url:"sortby,omitempty"`
	SortOrder        string    `url:"sortorder,omitempty"`
	ThreatIDs        []string  `url:"threatIds,omitempty"`
	UserEmails       []string  `url:"useremails,omitempty"`
	UserIDs          []string  `url:"userids,omitempty"`
}

// GetActivities retrieves activities based on the provided options
func (c *Client) GetActivities(ctx context.Context, opts *GetActivitiesOptions) (*ActivitiesResponse, error) {
	endpoint := fmt.Sprintf("%s/web/api/v2.1/activities", c.baseURL)

	// Build query parameters
	query := url.Values{}
	if opts != nil {
		if len(opts.AccountIDs) > 0 {
			query.Set("accountIds", strings.Join(opts.AccountIDs, ","))
		}
		if len(opts.ActivityTypes) > 0 {
			actTypes := make([]string, len(opts.ActivityTypes))
			for i, t := range opts.ActivityTypes {
				actTypes[i] = fmt.Sprint(t)
			}
			query.Set("activitytypes", strings.Join(actTypes, ","))
		}
		// Add other options...
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add query parameters
	req.URL.RawQuery = query.Encode()

	// Add authentication
	req.Header.Set("Authorization", "ApiToken "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result ActivitiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// Alert related types and structures
type AgentDetectionInfo struct {
	AccountID   string `json:"accountId"`
	MachineType string `json:"machineType"`
	Name        string `json:"name"`
	OSFamily    string `json:"osFamily"`
	OSName      string `json:"osName"`
	OSRevision  string `json:"osRevision"`
	SiteID      string `json:"siteId"`
	UUID        string `json:"uuid"`
	Version     string `json:"version"`
}

type AlertInfo struct {
	AlertID                        string    `json:"alertId"`
	AnalystVerdict                 string    `json:"analystVerdict"`
	CreatedAt                      time.Time `json:"createdAt"`
	DNSRequest                     string    `json:"dnsRequest"`
	DNSResponse                    string    `json:"dnsResponse"`
	DstIP                          string    `json:"dstIp"`
	DstPort                        string    `json:"dstPort"`
	DVEventID                      string    `json:"dvEventId"`
	EventType                      string    `json:"eventType"`
	HitType                        string    `json:"hitType"`
	IncidentStatus                 string    `json:"incidentStatus"`
	IndicatorCategory              string    `json:"indicatorCategory"`
	IndicatorDescription           string    `json:"indicatorDescription"`
	IndicatorName                  string    `json:"indicatorName"`
	IsEDR                          bool      `json:"isEdr"`
	LoginAccountDomain             string    `json:"loginAccountDomain"`
	LoginAccountSid                string    `json:"loginAccountSid"`
	LoginIsAdministratorEquivalent string    `json:"loginIsAdministratorEquivalent"`
	LoginIsSuccessful              string    `json:"loginIsSuccessful"`
	LoginsUserName                 string    `json:"loginsUserName"`
	LoginType                      string    `json:"loginType"`
	ModulePath                     string    `json:"modulePath"`
	ModuleSha1                     string    `json:"moduleSha1"`
	NetEventDirection              string    `json:"netEventDirection"`
	RegistryKeyPath                string    `json:"registryKeyPath"`
	RegistryOldValue               string    `json:"registryOldValue"`
	RegistryOldValueType           string    `json:"registryOldValueType"`
	RegistryPath                   string    `json:"registryPath"`
	RegistryValue                  string    `json:"registryValue"`
	ReportedAt                     time.Time `json:"reportedAt"`
	Source                         string    `json:"source"`
	SrcIP                          string    `json:"srcIp"`
	SrcMachineIP                   string    `json:"srcMachineIp"`
	SrcPort                        string    `json:"srcPort"`
	TIndicatorComparisonMethod     string    `json:"tIndicatorComparisonMethod"`
	TIndicatorSource               string    `json:"tIndicatorSource"`
	TIndicatorType                 string    `json:"tIndicatorType"`
	TIndicatorValue                string    `json:"tIndicatorValue"`
	UpdatedAt                      time.Time `json:"updatedAt"`
}

type ContainerInfo struct {
	ID     string `json:"id"`
	Image  string `json:"image"`
	Labels string `json:"labels"`
	Name   string `json:"name"`
}

type KubernetesInfo struct {
	Cluster          string `json:"cluster"`
	ControllerKind   string `json:"controllerKind"`
	ControllerLabels string `json:"controllerLabels"`
	ControllerName   string `json:"controllerName"`
	Namespace        string `json:"namespace"`
	NamespaceLabels  string `json:"namespaceLabels"`
	Node             string `json:"node"`
	Pod              string `json:"pod"`
	PodLabels        string `json:"podLabels"`
}

type RuleInfo struct {
	S1QL          string `json:"s1ql"`
	Description   string `json:"description"`
	ID            string `json:"id"`
	Name          string `json:"name"`
	QueryLang     string `json:"queryLang"`
	QueryType     string `json:"queryType"`
	ScopeLevel    string `json:"scopeLevel"`
	Severity      string `json:"severity"`
	TreatAsThreat string `json:"treatAsThreat"`
}

type ProcessInfo struct {
	IntegrityLevel     string `json:"integrityLevel"`
	Subsystem          string `json:"subsystem"`
	CommandLine        string `json:"commandline"`
	EffectiveUser      string `json:"effectiveUser"`
	FileHashMd5        string `json:"fileHashMd5"`
	FileHashSha1       string `json:"fileHashSha1"`
	FileHashSha256     string `json:"fileHashSha256"`
	FilePath           string `json:"filePath"`
	FileSignerIdentity string `json:"fileSignerIdentity"`
	LoginUser          string `json:"loginUser"`
	Name               string `json:"name"`
	PID                string `json:"pid"`
	PIDStartTime       string `json:"pidStarttime"`
	RealUser           string `json:"realUser"`
	Storyline          string `json:"storyline"`
	UniqueID           string `json:"uniqueId"`
	User               string `json:"user"`
}

type TargetProcessInfo struct {
	TgtFileCreatedAt      time.Time `json:"tgtFileCreatedAt"`
	TgtFileHashSha1       string    `json:"tgtFileHashSha1"`
	TgtFileHashSha256     string    `json:"tgtFileHashSha256"`
	TgtFileID             string    `json:"tgtFileId"`
	TgtFileIsSigned       string    `json:"tgtFileIsSigned"`
	TgtFileModifiedAt     time.Time `json:"tgtFileModifiedAt"`
	TgtFileOldPath        string    `json:"tgtFileOldPath"`
	TgtFilePath           string    `json:"tgtFilePath"`
	TgtProcCmdLine        string    `json:"tgtProcCmdLine"`
	TgtProcessStartTime   time.Time `json:"tgtProcessStartTime"`
	TgtProcImagePath      string    `json:"tgtProcImagePath"`
	TgtProcIntegrityLevel string    `json:"tgtProcIntegrityLevel"`
	TgtProcName           string    `json:"tgtProcName"`
	TgtProcPID            string    `json:"tgtProcPid"`
	TgtProcSignedStatus   string    `json:"tgtProcSignedStatus"`
	TgtProcStorylineID    string    `json:"tgtProcStorylineId"`
	TgtProcUID            string    `json:"tgtProcUid"`
}

type Alert struct {
	AgentDetectionInfo      AgentDetectionInfo `json:"agentDetectionInfo"`
	AlertInfo               AlertInfo          `json:"alertInfo"`
	ContainerInfo           ContainerInfo      `json:"containerInfo"`
	KubernetesInfo          KubernetesInfo     `json:"kubernetesInfo"`
	RuleInfo                RuleInfo           `json:"ruleInfo"`
	SourceParentProcessInfo ProcessInfo        `json:"sourceParentProcessInfo"`
	SourceProcessInfo       ProcessInfo        `json:"sourceProcessInfo"`
	TargetProcessInfo       TargetProcessInfo  `json:"targetProcessInfo"`
}

type AlertsResponse struct {
	Pagination PaginationData `json:"pagination"`
	Data       []Alert        `json:"data"`
	Errors     []string       `json:"errors"`
}

// GetAlertsOptions represents the available options for filtering alerts
type GetAlertsOptions struct {
	AccountIDs                          []string  `url:"accountIds,omitempty"`
	AnalystVerdict                      string    `url:"analystverdict,omitempty"`
	ContainerImageNameContains          []string  `url:"containerimagename__contains,omitempty"`
	ContainerLabelsContains             []string  `url:"containerlabels__contains,omitempty"`
	ContainerNameContains               []string  `url:"containername__contains,omitempty"`
	CountOnly                           bool      `url:"countonly,omitempty"`
	CreatedAtGT                         time.Time `url:"createdat__gt,omitempty"`
	CreatedAtGTE                        time.Time `url:"createdat__gte,omitempty"`
	CreatedAtLT                         time.Time `url:"createdat__lt,omitempty"`
	CreatedAtLTE                        time.Time `url:"createdat__lte,omitempty"`
	Cursor                              string    `url:"cursor,omitempty"`
	DisablePagination                   bool      `url:"disablepagination,omitempty"`
	GroupIDs                            []string  `url:"groupids,omitempty"`
	IDs                                 []string  `url:"ids,omitempty"`
	IncidentStatus                      string    `url:"incidentstatus,omitempty"`
	K8sClusterContains                  []string  `url:"k8scluster__contains,omitempty"`
	K8sControllerLabelsContains         []string  `url:"k8scontrollerlabels__contains,omitempty"`
	K8sControllerNameContains           string    `url:"k8scontrollername__contains,omitempty"`
	K8sNamespaceLabelsContains          []string  `url:"k8snamespacelabels__contains,omitempty"`
	K8sNamespaceNameContains            string    `url:"k8snamespacename__contains,omitempty"`
	K8sNodeContains                     string    `url:"k8snode__contains,omitempty"`
	K8sPodContains                      string    `url:"k8spod__contains,omitempty"`
	K8sPodLabelsContains                []string  `url:"k8spodlabels__contains,omitempty"`
	Limit                               int       `url:"limit,omitempty"`
	MachineType                         string    `url:"machinetype,omitempty"`
	OrigAgentNameContains               string    `url:"origagentname__contains,omitempty"`
	OrigAgentOSRevisionContains         string    `url:"origagentosrevision__contains,omitempty"`
	OrigAgentUUIDContains               string    `url:"origagentuuid__contains,omitempty"`
	OrigAgentVersionContains            string    `url:"origagentversion__contains,omitempty"`
	OSType                              string    `url:"ostype,omitempty"`
	Query                               string    `url:"query,omitempty"`
	ReportedAtGT                        time.Time `url:"reportedat__gt,omitempty"`
	ReportedAtGTE                       time.Time `url:"reportedat__gte,omitempty"`
	ReportedAtLT                        time.Time `url:"reportedat__lt,omitempty"`
	ReportedAtLTE                       time.Time `url:"reportedat__lte,omitempty"`
	RuleNameContains                    string    `url:"rulename__contains,omitempty"`
	Scopes                              []string  `url:"scopes,omitempty"`
	Severity                            string    `url:"severity,omitempty"`
	SiteIDs                             []string  `url:"siteids,omitempty"`
	Skip                                int       `url:"skip,omitempty"`
	SkipCount                           bool      `url:"skipcount,omitempty"`
	SortBy                              string    `url:"sortby,omitempty"`
	SortOrder                           string    `url:"sortorder,omitempty"`
	SourceProcessCommandLineContains    string    `url:"sourceprocesscommandline__contains,omitempty"`
	SourceProcessFileHashMD5Contains    string    `url:"sourceprocessfilehashmd5__contains,omitempty"`
	SourceProcessFileHashSHA1Contains   string    `url:"sourceprocessfilehashsha1__contains,omitempty"`
	SourceProcessFileHashSHA256Contains string    `url:"sourceprocessfilehashsha256__contains,omitempty"`
	SourceProcessFilePathContains       string    `url:"sourceprocessfilepath__contains,omitempty"`
	SourceProcessNameContains           string    `url:"sourceprocessname__contains,omitempty"`
	SourceProcessStorylineContains      string    `url:"sourceprocessstoryline__contains,omitempty"`
	Tenant                              bool      `url:"tenant,omitempty"`
}

// GetAlerts retrieves alerts based on the provided options
func (c *Client) GetAlerts(ctx context.Context, opts *GetAlertsOptions) (*AlertsResponse, error) {
	endpoint := fmt.Sprintf("%s/web/api/v2.1/cloud-detection/alerts", c.baseURL)

	// Build query parameters
	query := url.Values{}
	if opts != nil {
		if len(opts.AccountIDs) > 0 {
			query.Set("accountIds", strings.Join(opts.AccountIDs, ","))
		}
		if opts.AnalystVerdict != "" {
			query.Set("analystverdict", opts.AnalystVerdict)
		}
		if len(opts.ContainerImageNameContains) > 0 {
			query.Set("containerimagename__contains", strings.Join(opts.ContainerImageNameContains, ","))
		}
		// Add other options...
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add query parameters
	req.URL.RawQuery = query.Encode()

	// Add authentication
	req.Header.Set("Authorization", "ApiToken "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result AlertsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// Threat related types and structures
type Threat struct {
	ID                    string    `json:"id"`
	AgentID               string    `json:"agentId"`
	AgentComputerName     string    `json:"computerName"`
	AgentDomain           string    `json:"detectionAgentDomain"`
	AgentVersion          string    `json:"detectionAgentVersion"`
	AnalystVerdict        string    `json:"analystVerdict"`
	Classification        string    `json:"classification"`
	ClassificationSource  string    `json:"classificationSource"`
	CloudAccount          string    `json:"cloudAccount"`
	CloudImage            string    `json:"cloudImage"`
	CloudInstanceID       string    `json:"cloudInstanceId"`
	CloudInstanceSize     string    `json:"cloudInstanceSize"`
	CloudLocation         string    `json:"cloudLocation"`
	CloudNetwork          string    `json:"cloudNetwork"`
	CloudProvider         string    `json:"cloudProvider"`
	CommandLineArguments  string    `json:"commandLineArguments"`
	ConfidenceLevel       string    `json:"confidenceLevel"`
	ContainerImageName    string    `json:"containerImageName"`
	ContainerLabels       string    `json:"containerLabels"`
	ContainerName         string    `json:"containerName"`
	ContentHash           string    `json:"contentHash"`
	CreatedAt             time.Time `json:"createdAt"`
	DisplayName           string    `json:"displayName"`
	Engine                string    `json:"engine"`
	ExternalTicketID      string    `json:"externalTicketId"`
	FilePath              string    `json:"filePath"`
	HasAgentTags          bool      `json:"hasAgentTags"`
	IncidentStatus        string    `json:"incidentStatus"`
	InitiatedBy           string    `json:"initiatedBy"`
	InitiatedByUsername   string    `json:"initiatedByUsername"`
	K8sClusterName        string    `json:"k8sClusterName"`
	K8sControllerLabels   string    `json:"k8sControllerLabels"`
	K8sControllerName     string    `json:"k8sControllerName"`
	K8sNamespaceLabels    string    `json:"k8sNamespaceLabels"`
	K8sNamespaceName      string    `json:"k8sNamespaceName"`
	K8sNodeLabels         string    `json:"k8sNodeLabels"`
	K8sNodeName           string    `json:"k8sNodeName"`
	K8sPodLabels          string    `json:"k8sPodLabels"`
	K8sPodName            string    `json:"k8sPodName"`
	MitigatedPreemptively bool      `json:"mitigatedPreemptively"`
	MitigationStatus      string    `json:"mitigationStatus"`
	OriginatedProcess     string    `json:"originatedProcess"`
	OSArch                string    `json:"osArch"`
	OSName                string    `json:"osName"`
	OSType                string    `json:"osType"`
	PublisherName         string    `json:"publisherName"`
	RealtimeAgentVersion  string    `json:"realtimeAgentVersion"`
	RebootRequired        bool      `json:"rebootRequired"`
	Resolved              bool      `json:"resolved"`
	SiteID                string    `json:"siteId"`
	Storyline             string    `json:"storyline"`
	ThreatDetails         string    `json:"threatDetails"`
	UpdatedAt             time.Time `json:"updatedAt"`
	UUID                  string    `json:"uuid"`
}

type ThreatsResponse struct {
	Pagination PaginationData `json:"pagination"`
	Data       []Threat       `json:"data"`
	Errors     []string       `json:"errors"`
}

// GetThreatsOptions represents the available options for filtering threats
type GetThreatsOptions struct {
	AccountIDs                    []string  `url:"accountIds,omitempty"`
	AgentIDs                      []string  `url:"agentIds,omitempty"`
	AgentsActive                  *bool     `url:"agentsactive,omitempty"`
	AgentMachineTypes             []string  `url:"agentmachinetypes,omitempty"`
	AgentMachineTypesNin          []string  `url:"agentmachinetypesnin,omitempty"`
	AgentTagsData                 string    `url:"agentagsdata,omitempty"`
	AgentVersions                 []string  `url:"agentversions,omitempty"`
	AgentVersionsNin              []string  `url:"agentversionsnin,omitempty"`
	AnalystVerdicts               []string  `url:"analystverdicts,omitempty"`
	AnalystVerdictsNin            []string  `url:"analystverdictsnin,omitempty"`
	AWSRoleContains               []string  `url:"awsrole__contains,omitempty"`
	AWSSecurityGroupsContains     []string  `url:"awssecuritygroups__contains,omitempty"`
	AWSSubnetIDsContains          []string  `url:"awssubnetids__contains,omitempty"`
	AzureResourceGroupContains    []string  `url:"azureresourcegroup__contains,omitempty"`
	Classifications               []string  `url:"classifications,omitempty"`
	ClassificationsNin            []string  `url:"classificationsnin,omitempty"`
	ClassificationSources         []string  `url:"classificationsources,omitempty"`
	ClassificationSourcesNin      []string  `url:"classificationsourcesnin,omitempty"`
	CloudAccountContains          []string  `url:"cloudaccount__contains,omitempty"`
	CloudImageContains            []string  `url:"cloudimage__contains,omitempty"`
	CloudInstanceIDContains       []string  `url:"cloudinstanceid__contains,omitempty"`
	CloudInstanceSizeContains     []string  `url:"cloudinstancesize__contains,omitempty"`
	CloudLocationContains         []string  `url:"cloudlocation__contains,omitempty"`
	CloudNetworkContains          []string  `url:"cloudnetwork__contains,omitempty"`
	CloudProvider                 string    `url:"cloudprovider,omitempty"`
	CloudProviderNin              string    `url:"cloudprovidernin,omitempty"`
	CollectionIDs                 []string  `url:"collectionids,omitempty"`
	CommandLineArgumentsContains  []string  `url:"commandlinearguments__contains,omitempty"`
	ComputerNameContains          []string  `url:"computername__contains,omitempty"`
	ConfidenceLevels              []string  `url:"confidencelevels,omitempty"`
	ConfidenceLevelsNin           []string  `url:"confidencelevelsnin,omitempty"`
	ContainerImageNameContains    []string  `url:"containerimagename__contains,omitempty"`
	ContainerLabelsContains       []string  `url:"containerlabels__contains,omitempty"`
	ContainerNameContains         []string  `url:"containername__contains,omitempty"`
	ContentHashContains           []string  `url:"contenthash__contains,omitempty"`
	ContentHashes                 []string  `url:"contenthashes,omitempty"`
	CountOnly                     bool      `url:"countonly,omitempty"`
	CountsFor                     []string  `url:"countsfor,omitempty"`
	CreatedAtGT                   time.Time `url:"createdat__gt,omitempty"`
	CreatedAtGTE                  time.Time `url:"createdat__gte,omitempty"`
	CreatedAtLT                   time.Time `url:"createdat__lt,omitempty"`
	CreatedAtLTE                  time.Time `url:"createdat__lte,omitempty"`
	Cursor                        string    `url:"cursor,omitempty"`
	DetectionAgentDomainContains  []string  `url:"detectionagentdomain__contains,omitempty"`
	DetectionAgentVersionContains []string  `url:"detectionagentversion__contains,omitempty"`
	DetectionEngines              []string  `url:"detectionengines,omitempty"`
	DetectionEnginesNin           []string  `url:"detectionenginesnin,omitempty"`
	DisplayName                   string    `url:"displayname,omitempty"`
	Engines                       []string  `url:"engines,omitempty"`
	EnginesNin                    []string  `url:"enginesnin,omitempty"`
	ExternalTicketExists          *bool     `url:"externalticketexists,omitempty"`
	ExternalTicketIDContains      []string  `url:"externalticketid__contains,omitempty"`
	ExternalTicketIDs             []string  `url:"externalticketids,omitempty"`
	FailedActions                 *bool     `url:"failedactions,omitempty"`
	FilePathContains              []string  `url:"filepath__contains,omitempty"`
	GCPServiceAccountContains     []string  `url:"gcpserviceaccount__contains,omitempty"`
	GroupIDs                      []string  `url:"groupids,omitempty"`
	HasAgentTags                  *bool     `url:"hasagenttags,omitempty"`
	IDs                           []string  `url:"ids,omitempty"`
	IncidentStatuses              []string  `url:"incidentstatuses,omitempty"`
	IncidentStatusesNin           []string  `url:"incidentstatusesnin,omitempty"`
	InitiatedBy                   []string  `url:"initiatedby,omitempty"`
	InitiatedByNin                []string  `url:"initatedbynin,omitempty"`
	InitiatedByUsernameContains   []string  `url:"initiatedbyusername__contains,omitempty"`
	K8sClusterNameContains        []string  `url:"k8sclustername__contains,omitempty"`
	K8sControllerLabelsContains   []string  `url:"k8scontrollerlabels__contains,omitempty"`
	K8sControllerNameContains     []string  `url:"k8scontrollername__contains,omitempty"`
	K8sNamespaceLabelsContains    []string  `url:"k8snamespacelabels__contains,omitempty"`
	K8sNamespaceNameContains      []string  `url:"k8snamespacename__contains,omitempty"`
	K8sNodeLabelsContains         []string  `url:"k8snodelabels__contains,omitempty"`
	K8sNodeNameContains           []string  `url:"k8snodename__contains,omitempty"`
	K8sPodLabelsContains          []string  `url:"k8spodlabels__contains,omitempty"`
	K8sPodNameContains            []string  `url:"k8spodname__contains,omitempty"`
	Limit                         int       `url:"limit,omitempty"`
	MitigatedPreemptively         *bool     `url:"mitigatedpreemptively,omitempty"`
	MitigationStatuses            []string  `url:"mitigationstatuses,omitempty"`
	MitigationStatusesNin         []string  `url:"mitigationstatusesnin,omitempty"`
	NoteExists                    *bool     `url:"noteexists,omitempty"`
	OriginatedProcessContains     []string  `url:"originatedprocess__contains,omitempty"`
	OSArchs                       []string  `url:"osarchs,omitempty"`
	OSNames                       []string  `url:"osnames,omitempty"`
	OSNamesNin                    []string  `url:"osnamesnin,omitempty"`
	OSTypes                       []string  `url:"ostypes,omitempty"`
	OSTypesNin                    []string  `url:"ostypesnin,omitempty"`
	PendingActions                *bool     `url:"pendingactions,omitempty"`
	PublisherNameContains         []string  `url:"publishername__contains,omitempty"`
	Query                         string    `url:"query,omitempty"`
	RealtimeAgentVersionContains  []string  `url:"realtimeagentversion__contains,omitempty"`
	RebootRequired                *bool     `url:"rebootrequired,omitempty"`
	Resolved                      *bool     `url:"resolved,omitempty"`
	SiteIDs                       []string  `url:"siteids,omitempty"`
	Skip                          int       `url:"skip,omitempty"`
	SkipCount                     bool      `url:"skipcount,omitempty"`
	SortBy                        string    `url:"sortby,omitempty"`
	SortOrder                     string    `url:"sortorder,omitempty"`
	StorylineContains             []string  `url:"storyline__contains,omitempty"`
	Storylines                    []string  `url:"storylines,omitempty"`
	Tenant                        bool      `url:"tenant,omitempty"`
	ThreatDetailsContains         []string  `url:"threatdetails__contains,omitempty"`
	UpdatedAtGT                   time.Time `url:"updatedat__gt,omitempty"`
	UpdatedAtGTE                  time.Time `url:"updatedat__gte,omitempty"`
	UpdatedAtLT                   time.Time `url:"updatedat__lt,omitempty"`
	UpdatedAtLTE                  time.Time `url:"updatedat__lte,omitempty"`
	UUIDContains                  []string  `url:"uuid__contains,omitempty"`
}

// GetThreats retrieves threats based on the provided options
func (c *Client) GetThreats(ctx context.Context, opts *GetThreatsOptions) (*ThreatsResponse, error) {
	endpoint := fmt.Sprintf("%s/web/api/v2.1/threats", c.baseURL)

	// Build query parameters
	query := url.Values{}
	if opts != nil {
		if len(opts.AccountIDs) > 0 {
			query.Set("accountIds", strings.Join(opts.AccountIDs, ","))
		}
		if len(opts.AgentIDs) > 0 {
			query.Set("agentIds", strings.Join(opts.AgentIDs, ","))
		}
		if opts.AgentsActive != nil {
			query.Set("agentsactive", fmt.Sprintf("%v", *opts.AgentsActive))
		}
		if len(opts.AgentMachineTypes) > 0 {
			query.Set("agentmachinetypes", strings.Join(opts.AgentMachineTypes, ","))
		}
		if len(opts.AnalystVerdicts) > 0 {
			query.Set("analystverdicts", strings.Join(opts.AnalystVerdicts, ","))
		}
		if opts.CreatedAtGT.IsZero() == false {
			query.Set("createdat__gt", opts.CreatedAtGT.Format(time.RFC3339))
		}
		// Add other options...
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add query parameters
	req.URL.RawQuery = query.Encode()

	// Add authentication
	req.Header.Set("Authorization", "ApiToken "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result ThreatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}
