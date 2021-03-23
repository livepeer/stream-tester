package recordtester

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/PagerDuty/go-pagerduty"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/messenger"
)

type (
	// IContinuousRecordTester ...
	IContinuousRecordTester interface {
		// Start start test. Blocks until error.
		Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error
		Cancel()
		Done() <-chan struct{}
	}

	continuousRecordTester struct {
		lapi                    *livepeer.API
		ctx                     context.Context
		cancel                  context.CancelFunc
		host                    string // API host being tested
		pagerDutyIntegrationKey string
		pagerDutyComponent      string
	}
)

// NewContinuousRecordTester returns new object
func NewContinuousRecordTester(gctx context.Context, lapi *livepeer.API, pagerDutyIntegrationKey, pagerDutyComponent string) IContinuousRecordTester {
	ctx, cancel := context.WithCancel(gctx)
	server := lapi.GetServer()
	u, _ := url.Parse(server)
	crt := &continuousRecordTester{
		lapi:                    lapi,
		ctx:                     ctx,
		cancel:                  cancel,
		host:                    u.Host,
		pagerDutyIntegrationKey: pagerDutyIntegrationKey,
		pagerDutyComponent:      pagerDutyComponent,
	}
	return crt
}

func (crt *continuousRecordTester) Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error {
	glog.Infof("Starting continuous test of %s", crt.host)
	try := 0
	for {
		msg := fmt.Sprintf(":arrow_right: Starting %s recordings test stream to %s", 2*testDuration, crt.host)
		glog.Info(msg)
		messenger.SendMessage(msg)
		rt := NewRecordTester(crt.ctx, crt.lapi, true)
		es, err := rt.Start(fileName, testDuration, pauseDuration)
		if err == context.Canceled {
			msg := fmt.Sprintf("Test of %s cancelled", crt.host)
			messenger.SendMessage(msg)
			return err
		} else if err != nil || es != 0 {
			var re *testers.RTMPError
			if errors.As(err, &re) && try == 0 {
				msg := fmt.Sprintf(":rotating_light: Test of %s ended with RTMP err=%v errCode=%v try=0, trying second time", crt.host, err, es)
				messenger.SendMessage(msg)
				rt.Clean()
				try++
				time.Sleep(10 * time.Second)
				continue
			}
			msg := fmt.Sprintf(":rotating_light: Test of %s ended with err=%v errCode=%v", crt.host, err, es)
			messenger.SendFatalMessage(msg)
			glog.Warning(msg)
			if crt.pagerDutyIntegrationKey != "" {
				event := pagerduty.V2Event{
					RoutingKey: crt.pagerDutyIntegrationKey,
					Action:     "trigger",
					Payload: &pagerduty.V2Payload{
						Source:    crt.host,
						Component: crt.pagerDutyComponent,
						Severity:  "error",
						Summary:   crt.host + ": " + err.Error(),
					},
				}
				sid := rt.StreamID()
				if sid != "" {
					link := "https://livepeer.com/app/stream/" + sid
					event.Links = append(event.Links, link)
				}
				resp, err := pagerduty.ManageEvent(event)
				if err != nil {
					glog.Error(err)
				} else {
					glog.Infof("Incident status: %s message: %s", resp.Status, resp.Message)
				}
			}
		} else {
			msg := fmt.Sprintf(":white_check_mark: Test of %s succeed", crt.host)
			messenger.SendMessage(msg)
			glog.Warning(msg)
		}
		try = 0
		rt.Clean()
		glog.Infof("Waiting %s before next test", pauseBetweenTests)
		time.Sleep(pauseBetweenTests)
		select {
		case <-crt.ctx.Done():
			return context.Canceled
		default:
		}
	}
}

func (crt *continuousRecordTester) Cancel() {
	crt.cancel()
}

func (crt *continuousRecordTester) Done() <-chan struct{} {
	return crt.ctx.Done()
}
