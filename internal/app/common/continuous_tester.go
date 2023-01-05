package common

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/PagerDuty/go-pagerduty"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/messenger"
)

type (
	IContinuousTester interface {
		// Start test. Blocks until error.
		Start(start func(ctx context.Context) (int, error), testDuration, pauseBetweenTests time.Duration) error
		Cancel()
		Done() <-chan struct{}
	}

	ContinuousTesterOptions struct {
		PagerDutyIntegrationKey string
		PagerDutyComponent      string
		PagerDutyLowUrgency     bool
		TesterOptions
	}

	continuousTester struct {
		ctx                     context.Context
		cancel                  context.CancelFunc
		host                    string // API host being tested
		pagerDutyIntegrationKey string
		pagerDutyComponent      string
		pagerDutyLowUrgency     bool
		name                    string
	}
)

func NewContinuousTester(gctx context.Context, opts ContinuousTesterOptions, testerName string) IContinuousTester {
	ctx, cancel := context.WithCancel(gctx)
	server := opts.API.GetServer()
	u, _ := url.Parse(server)
	ct := &continuousTester{
		ctx:                     ctx,
		cancel:                  cancel,
		host:                    u.Host,
		pagerDutyIntegrationKey: opts.PagerDutyIntegrationKey,
		pagerDutyComponent:      opts.PagerDutyComponent,
		pagerDutyLowUrgency:     opts.PagerDutyLowUrgency,
		name:                    testerName,
	}
	return ct
}

func (ct *continuousTester) Start(start func(ctx context.Context) (int, error), testDuration, pauseBetweenTests time.Duration) error {
	messenger.SendMessage(fmt.Sprintf("Starting continuous %s test of %s", ct.name, ct.host))
	for {
		msg := fmt.Sprintf(":arrow_right: Starting %s %s test to %s", testDuration, ct.name, ct.host)
		messenger.SendMessage(msg)

		ctx, cancel := context.WithTimeout(ct.ctx, testDuration)
		es, err := start(ctx)
		ctxErr := ctx.Err()
		cancel()

		if ct.ctx.Err() != nil {
			messenger.SendMessage(fmt.Sprintf("Continuous test of %s on %s cancelled", ct.name, ct.host))
			return ct.ctx.Err()
		} else if ctxErr != nil {
			msg := fmt.Sprintf("Test of %s on %s timed out, potential deadlock! ctxErr=%q err=%q", ct.name, ct.host, ctxErr, err)
			messenger.SendFatalMessage(msg)
		} else if err != nil || es != 0 {
			msg := fmt.Sprintf(":rotating_light: Test of %s on %s ended with err=%v errCode=%v", ct.name, ct.host, err, es)
			messenger.SendFatalMessage(msg)
			glog.Warning(msg)
			ct.sendPagerdutyEvent(err)
		} else {
			msg := fmt.Sprintf(":white_check_mark: Test of %s on %s succeeded", ct.name, ct.host)
			messenger.SendMessage(msg)
			glog.Info(msg)
			ct.sendPagerdutyEvent(nil)
		}
		glog.Infof("Waiting %s before next test of %s", pauseBetweenTests, ct.name)
		select {
		case <-ct.ctx.Done():
			messenger.SendMessage(fmt.Sprintf("Continuous test of %s on %s cancelled", ct.name, ct.host))
			return err
		case <-time.After(pauseBetweenTests):
		}
	}
	return nil
}

func (ct *continuousTester) sendPagerdutyEvent(err error) {
	if ct.pagerDutyIntegrationKey == "" {
		return
	}
	severity, lopriPrefix, dedupKey := "error", "", fmt.Sprintf("cont-%s-tester:%s", ct.name, ct.host)
	if ct.pagerDutyLowUrgency {
		severity, lopriPrefix = "warning", "[LOPRI] "
		dedupKey = "lopri-" + dedupKey
	}
	event := pagerduty.V2Event{
		RoutingKey: ct.pagerDutyIntegrationKey,
		Action:     "trigger",
		DedupKey:   dedupKey,
	}
	if err == nil {
		event.Action = "resolve"
		_, err := pagerduty.ManageEvent(event)
		if err != nil {
			messenger.SendMessage(fmt.Sprintf("Error resolving PagerDuty event: %v", err))
		}
		return
	}
	summary := fmt.Sprintf("%s:vhs: %s %s for `%s` error: %v", lopriPrefix, ct.name, ct.pagerDutyComponent, ct.host, err)
	if len(summary) > 1024 {
		summary = summary[:1021] + "..."
	}
	event.Payload = &pagerduty.V2Payload{
		Source:    ct.host,
		Component: ct.pagerDutyComponent,
		Severity:  severity,
		Summary:   summary,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	resp, err := pagerduty.ManageEvent(event)
	if err != nil {
		glog.Error(fmt.Errorf("PAGERDUTY Error: %w", err))
		messenger.SendFatalMessage(fmt.Sprintf("Error creating PagerDuty event: %v", err))
	} else {
		glog.Infof("Incident status: %s message: %s", resp.Status, resp.Message)
	}
}

func (ct *continuousTester) Cancel() {
	ct.cancel()
}

func (ct *continuousTester) Done() <-chan struct{} {
	return ct.ctx.Done()
}
