package transcodetester

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/PagerDuty/go-pagerduty"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/app/common"
	"github.com/livepeer/stream-tester/messenger"
)

type (
	IContinuousTranscodeTester interface {
		// Start test. Blocks until error.
		Start(fileName string, transcodeBucketUrl string, testDuration, taskPollDuration, pauseBetweenTests time.Duration) error
		Cancel()
		Done() <-chan struct{}
	}

	ContinuousTranscodeTesterOptions struct {
		PagerDutyIntegrationKey string
		PagerDutyComponent      string
		PagerDutyLowUrgency     bool
		common.TesterOptions
	}

	continuousTranscodeTester struct {
		ctx                     context.Context
		cancel                  context.CancelFunc
		host                    string // API host being tested
		pagerDutyIntegrationKey string
		pagerDutyComponent      string
		pagerDutyLowUrgency     bool
		ttOpts                  common.TesterOptions
	}
)

func NewContinuousTransTester(gctx context.Context, opts ContinuousTranscodeTesterOptions) IContinuousTranscodeTester {
	ctx, cancel := context.WithCancel(gctx)
	server := opts.API.GetServer()
	u, _ := url.Parse(server)
	ctt := &continuousTranscodeTester{
		ctx:                     ctx,
		cancel:                  cancel,
		host:                    u.Host,
		pagerDutyIntegrationKey: opts.PagerDutyIntegrationKey,
		pagerDutyComponent:      opts.PagerDutyComponent,
		pagerDutyLowUrgency:     opts.PagerDutyLowUrgency,
		ttOpts:                  opts.TesterOptions,
	}
	return ctt
}

func (ctt *continuousTranscodeTester) Start(fileName string, transcodeBucketUrl string, testDuration, taskPollDuration, pauseBetweenTests time.Duration) error {
	messenger.SendMessage(fmt.Sprintf("Starting continuous transcode test of %s", ctt.host))
	for {
		msg := fmt.Sprintf(":arrow_right: Starting %s transcode test to %s", testDuration, ctt.host)
		messenger.SendMessage(msg)

		ctx, cancel := context.WithTimeout(ctt.ctx, testDuration)
		tt := NewTranscodeTester(ctx, ctt.ttOpts)
		es, err := tt.Start(fileName, transcodeBucketUrl, taskPollDuration)
		ctxErr := ctx.Err()
		cancel()

		if ctt.ctx.Err() != nil {
			messenger.SendMessage(fmt.Sprintf("Continuous test of Transcode API on %s cancelled", ctt.host))
			return ctt.ctx.Err()
		} else if ctxErr != nil {
			msg := fmt.Sprintf("Test of Transcode API on %s timed out, potential deadlock! ctxErr=%q err=%q", ctt.host, ctxErr, err)
			messenger.SendFatalMessage(msg)
		} else if err != nil || es != 0 {
			msg := fmt.Sprintf(":rotating_light: Test of Transcode API on %s ended with err=%v errCode=%v", ctt.host, err, es)
			messenger.SendFatalMessage(msg)
			glog.Warning(msg)
			ctt.sendPagerdutyEvent(tt, err)
		} else {
			msg := fmt.Sprintf(":white_check_mark: Test of Transcode API on %s succeeded", ctt.host)
			messenger.SendMessage(msg)
			glog.Info(msg)
			ctt.sendPagerdutyEvent(tt, nil)
		}
		glog.Infof("Waiting %s before next test of Transcode API", pauseBetweenTests)
		select {
		case <-ctt.ctx.Done():
			messenger.SendMessage(fmt.Sprintf("Continuous test of Transcode API on %s cancelled", ctt.host))
			return err
		case <-time.After(pauseBetweenTests):
		}
	}
	return nil
}

// TODO: Refactor to reuse with VOD pager duty event
func (ctt *continuousTranscodeTester) sendPagerdutyEvent(tt ITranscodeTester, err error) {
	if ctt.pagerDutyIntegrationKey == "" {
		return
	}
	severity, lopriPrefix, dedupKey := "error", "", fmt.Sprintf("cont-transcode-tester:%s", ctt.host)
	if ctt.pagerDutyLowUrgency {
		severity, lopriPrefix = "warning", "[LOPRI] "
		dedupKey = "lopri-" + dedupKey
	}
	event := pagerduty.V2Event{
		RoutingKey: ctt.pagerDutyIntegrationKey,
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
	summary := fmt.Sprintf("%s:vhs: Transcode API %s for `%s` error: %v", lopriPrefix, ctt.pagerDutyComponent, ctt.host, err)
	if len(summary) > 1024 {
		summary = summary[:1021] + "..."
	}
	event.Payload = &pagerduty.V2Payload{
		Source:    ctt.host,
		Component: ctt.pagerDutyComponent,
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

func (ctt *continuousTranscodeTester) Cancel() {
	ctt.cancel()
}

func (ctt *continuousTranscodeTester) Done() <-chan struct{} {
	return ctt.ctx.Done()
}
