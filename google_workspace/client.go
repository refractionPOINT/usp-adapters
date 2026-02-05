package usp_google_workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/oauth2/google"
	admin "google.golang.org/api/admin/reports/v1"
	"google.golang.org/api/option"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

const (
	defaultPollInterval = 60 // seconds
	// Available Google Workspace applications for audit logs
	// https://developers.google.com/admin-sdk/reports/reference/rest/v1/activities/list
	appAdmin        = "admin"
	appCalendar     = "calendar"
	appChat         = "chat"
	appDrive        = "drive"
	appGCP          = "gcp"
	appGroups       = "groups"
	appGroupsEnt    = "groups_enterprise"
	appLogin        = "login"
	appMeet         = "meet"
	appMobile       = "mobile"
	appRules        = "rules"
	appSAML         = "saml"
	appToken        = "token"
	appUserAccounts = "user_accounts"
	appContext      = "context_aware_access"
	appChrome       = "chrome"
	appDataStudio   = "data_studio"
	appKeep         = "keep"
)

var validApplications = map[string]bool{
	appAdmin:        true,
	appCalendar:     true,
	appChat:         true,
	appDrive:        true,
	appGCP:          true,
	appGroups:       true,
	appGroupsEnt:    true,
	appLogin:        true,
	appMeet:         true,
	appMobile:       true,
	appRules:        true,
	appSAML:         true,
	appToken:        true,
	appUserAccounts: true,
	appContext:      true,
	appChrome:       true,
	appDataStudio:   true,
	appKeep:         true,
}

type GoogleWorkspaceAdapter struct {
	conf      GoogleWorkspaceConfig
	uspClient *uspclient.Client

	ctx context.Context

	service *admin.Service

	isStop uint32
	wg     sync.WaitGroup

	// Track last event time per application for deduplication
	lastEventTime map[string]time.Time
	lastTimeMu    sync.RWMutex
}

type GoogleWorkspaceConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ServiceAccountCreds string                  `json:"service_account_creds" yaml:"service_account_creds"`
	DelegatedAdmin      string                  `json:"delegated_admin" yaml:"delegated_admin"`
	CustomerID          string                  `json:"customer_id" yaml:"customer_id"`
	Applications        []string                `json:"applications" yaml:"applications"`
	PollIntervalSeconds int                     `json:"poll_interval_seconds" yaml:"poll_interval_seconds"`
}

func (c *GoogleWorkspaceConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ServiceAccountCreds == "" {
		return errors.New("missing service_account_creds")
	}
	if c.DelegatedAdmin == "" {
		return errors.New("missing delegated_admin (admin email for domain-wide delegation)")
	}
	if len(c.Applications) == 0 {
		return errors.New("missing applications (specify at least one: admin, login, drive, etc.)")
	}
	for _, app := range c.Applications {
		if !validApplications[strings.ToLower(app)] {
			return fmt.Errorf("invalid application: %s", app)
		}
	}
	c.ServiceAccountCreds = strings.TrimSpace(c.ServiceAccountCreds)
	return nil
}

func NewGoogleWorkspaceAdapter(ctx context.Context, conf GoogleWorkspaceConfig) (*GoogleWorkspaceAdapter, chan struct{}, error) {
	if conf.PollIntervalSeconds <= 0 {
		conf.PollIntervalSeconds = defaultPollInterval
	}

	// Default customer ID to "my_customer" if not specified
	if conf.CustomerID == "" {
		conf.CustomerID = "my_customer"
	}

	a := &GoogleWorkspaceAdapter{
		conf:          conf,
		ctx:           context.Background(),
		lastEventTime: make(map[string]time.Time),
	}

	var err error

	// Parse the service account credentials
	var creds []byte
	if strings.HasPrefix(conf.ServiceAccountCreds, "{") {
		// JSON credentials provided directly
		creds = []byte(conf.ServiceAccountCreds)
	} else {
		return nil, nil, errors.New("service_account_creds must be JSON credentials")
	}

	// Create JWT config with domain-wide delegation
	jwtConfig, err := google.JWTConfigFromJSON(creds, admin.AdminReportsAuditReadonlyScope)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse service account credentials: %v", err)
	}

	// Set the subject to the delegated admin for impersonation
	jwtConfig.Subject = conf.DelegatedAdmin

	// Create the Admin SDK Reports service
	a.service, err = admin.NewService(a.ctx, option.WithHTTPClient(jwtConfig.Client(a.ctx)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create admin service: %v", err)
	}

	// Create USP client
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	chStopped := make(chan struct{})

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(chStopped)

		// Initialize last event time to now minus poll interval to avoid re-fetching old events
		startTime := time.Now().UTC().Add(-time.Duration(conf.PollIntervalSeconds) * time.Second)
		for _, app := range conf.Applications {
			a.lastTimeMu.Lock()
			a.lastEventTime[strings.ToLower(app)] = startTime
			a.lastTimeMu.Unlock()
		}

		for {
			if atomic.LoadUint32(&a.isStop) == 1 {
				break
			}

			for _, app := range conf.Applications {
				if atomic.LoadUint32(&a.isStop) == 1 {
					break
				}

				if err := a.pollApplication(strings.ToLower(app)); err != nil {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("error polling %s: %v", app, err))
				}
			}

			// Sleep for poll interval
			for i := 0; i < conf.PollIntervalSeconds; i++ {
				if atomic.LoadUint32(&a.isStop) == 1 {
					break
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()

	return a, chStopped, nil
}

func (a *GoogleWorkspaceAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	atomic.StoreUint32(&a.isStop, 1)
	a.wg.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *GoogleWorkspaceAdapter) pollApplication(application string) error {
	a.lastTimeMu.RLock()
	lastTime := a.lastEventTime[application]
	a.lastTimeMu.RUnlock()

	// Format time for API (RFC3339)
	startTime := lastTime.Format(time.RFC3339)

	a.conf.ClientOptions.DebugLog(fmt.Sprintf("polling %s from %s", application, startTime))

	// Build the request
	req := a.service.Activities.List("all", application).
		CustomerId(a.conf.CustomerID).
		StartTime(startTime).
		MaxResults(1000)

	var newestTime time.Time
	eventCount := 0

	// Paginate through results
	err := req.Pages(a.ctx, func(resp *admin.Activities) error {
		if resp == nil {
			return nil
		}

		for _, activity := range resp.Items {
			if activity == nil {
				continue
			}

			// Parse the activity time
			activityTime, err := time.Parse(time.RFC3339, activity.Id.Time)
			if err != nil {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("failed to parse activity time: %v", err))
				continue
			}

			// Skip events we've already seen (equal or before lastTime)
			if !activityTime.After(lastTime) {
				continue
			}

			// Track newest event time
			if activityTime.After(newestTime) {
				newestTime = activityTime
			}

			// Process the event
			if err := a.processEvent(activity, application); err != nil {
				return err
			}
			eventCount++
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to list activities for %s: %v", application, err)
	}

	// Update last event time if we found newer events
	if !newestTime.IsZero() {
		a.lastTimeMu.Lock()
		a.lastEventTime[application] = newestTime
		a.lastTimeMu.Unlock()
	}

	if eventCount > 0 {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("processed %d events from %s", eventCount, application))
	}

	return nil
}

func (a *GoogleWorkspaceAdapter) processEvent(activity *admin.Activity, application string) error {
	// Enrich the activity with application name for easier processing
	enrichedActivity := map[string]interface{}{
		"application": application,
		"id":          activity.Id,
		"actor":       activity.Actor,
		"events":      activity.Events,
		"ip_address":  activity.IpAddress,
		"kind":        activity.Kind,
		"owner_domain": activity.OwnerDomain,
	}

	// Marshal to JSON
	data, err := json.Marshal(enrichedActivity)
	if err != nil {
		return fmt.Errorf("failed to marshal activity: %v", err)
	}

	msg := &protocol.DataMessage{
		TextPayload: string(data),
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}

	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			return err
		}
	}
	return nil
}
