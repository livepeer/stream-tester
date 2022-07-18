package vodtester

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
	// IContinuousVodTester ...
	IContinuousVodTester interface {
		// Start start test. Blocks until error.
		Start(fileName string, testDuration, taskPollDuration, pauseBetweenTests time.Duration) error
		Cancel()
		Done() <-chan struct{}
	}

	ContinuousVodTesterOptions struct {
		PagerDutyIntegrationKey string
		PagerDutyComponent      string
		PagerDutyLowUrgency     bool
		VodTesterOptions
	}

	continuousVodTester struct {
		ctx                     context.Context
		cancel                  context.CancelFunc
		host                    string // API host being tested
		pagerDutyIntegrationKey string
		pagerDutyComponent      string
		pagerDutyLowUrgency     bool
		vtOpts                  VodTesterOptions
	}
)

// NewContinuousVodTester returns new object
func NewContinuousVodTester(gctx context.Context, opts ContinuousVodTesterOptions) IContinuousVodTester {
	ctx, cancel := context.WithCancel(gctx)
	server := opts.API.GetServer()
	u, _ := url.Parse(server)
	cvt := &continuousVodTester{
		ctx:                     ctx,
		cancel:                  cancel,
		host:                    u.Host,
		pagerDutyIntegrationKey: opts.PagerDutyIntegrationKey,
		pagerDutyComponent:      opts.PagerDutyComponent,
		pagerDutyLowUrgency:     opts.PagerDutyLowUrgency,
		vtOpts:                  opts.VodTesterOptions,
	}
	return cvt
}

func (cvt *continuousVodTester) Start(fileName string, testDuration, taskPollDuration, pauseBetweenTests time.Duration) error {
	messenger.SendMessage(fmt.Sprintf("Starting continuous vod test of %s", cvt.host))
	ticker := time.NewTicker(testDuration)
	defer ticker.Stop()
	for range ticker.C {
		msg := fmt.Sprintf(":arrow_right: Starting %s vod test to %s", testDuration, cvt.host)
		messenger.SendMessage(msg)

		ctx, cancel := context.WithTimeout(cvt.ctx, testDuration)
		vt := NewVodTester(ctx, cvt.vtOpts)
		es, err := vt.Start(fileName, taskPollDuration)
		ctxErr := ctx.Err()
		cancel()

		if cvt.ctx.Err() != nil {
			messenger.SendMessage(fmt.Sprintf("Continuous test of VOD on %s cancelled", cvt.host))
			return cvt.ctx.Err()
		} else if ctxErr != nil {
			msg := fmt.Sprintf("Test of VOD on %s timed out, potential deadlock! ctxErr=%q err=%q", cvt.host, ctxErr, err)
			messenger.SendFatalMessage(msg)
		} else if err != nil || es != 0 {
			msg := fmt.Sprintf(":rotating_light: Test of VOD on %s ended with err=%v errCode=%v", cvt.host, err, es)
			messenger.SendFatalMessage(msg)
			glog.Warning(msg)
			cvt.sendPagerdutyEvent(vt, err)
		} else {
			msg := fmt.Sprintf(":white_check_mark: Test of VOD on %s succeeded", cvt.host)
			messenger.SendMessage(msg)
			glog.Info(msg)
			cvt.sendPagerdutyEvent(vt, nil)
		}
		glog.Infof("Waiting %s before next test of VOD", pauseBetweenTests)
		select {
		case <-cvt.ctx.Done():
			messenger.SendMessage(fmt.Sprintf("Continuous test of VOD on %s cancelled", cvt.host))
			return err
		case <-time.After(pauseBetweenTests):
		}
	}
	return nil
}

func (cvt *continuousVodTester) sendPagerdutyEvent(vt IVodTester, err error) {
	if cvt.pagerDutyIntegrationKey == "" {
		return
	}
	severity, lopriPrefix, dedupKey := "error", "", fmt.Sprintf("cont-vod-tester:%s", cvt.host)
	if cvt.pagerDutyLowUrgency {
		severity, lopriPrefix = "warning", "[LOPRI] "
		dedupKey = "lopri-" + dedupKey
	}
	event := pagerduty.V2Event{
		RoutingKey: cvt.pagerDutyIntegrationKey,
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
	event.Payload = &pagerduty.V2Payload{
		Source:    cvt.host,
		Component: cvt.pagerDutyComponent,
		Severity:  severity,
		Summary:   fmt.Sprintf("%s:vhs: VOD %s for `%s` error: %v", lopriPrefix, cvt.pagerDutyComponent, cvt.host, err),
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

func (cvt *continuousVodTester) Cancel() {
	cvt.cancel()
}

func (cvt *continuousVodTester) Done() <-chan struct{} {
	return cvt.ctx.Done()
}
