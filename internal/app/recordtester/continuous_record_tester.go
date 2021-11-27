package recordtester

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
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
		lanalyzers              testers.AnalyzerByRegion
		ctx                     context.Context
		cancel                  context.CancelFunc
		host                    string // API host being tested
		pagerDutyIntegrationKey string
		pagerDutyComponent      string
		useHTTP                 bool
		mp4                     bool
		streamHealth            bool
	}

	pagerDutyLink struct {
		Href string `json:"href,omitempty"`
		Text string `json:"text,omitempty"`
	}
)

// NewContinuousRecordTester returns new object
func NewContinuousRecordTester(gctx context.Context, lapi *livepeer.API, lanalyzers testers.AnalyzerByRegion, pagerDutyIntegrationKey, pagerDutyComponent string, useHTTP, mp4, streamHealth bool) IContinuousRecordTester {
	ctx, cancel := context.WithCancel(gctx)
	server := lapi.GetServer()
	u, _ := url.Parse(server)
	crt := &continuousRecordTester{
		lapi:                    lapi,
		lanalyzers:              lanalyzers,
		ctx:                     ctx,
		cancel:                  cancel,
		host:                    u.Host,
		pagerDutyIntegrationKey: pagerDutyIntegrationKey,
		pagerDutyComponent:      pagerDutyComponent,
		useHTTP:                 useHTTP,
		mp4:                     mp4,
		streamHealth:            streamHealth,
	}
	return crt
}

func (crt *continuousRecordTester) Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error {
	glog.Infof("Starting continuous test of %s", crt.host)
	try := 0
	notRtmpTry := 0
	for {
		msg := fmt.Sprintf(":arrow_right: Starting %s recordings test stream to %s", 2*testDuration, crt.host)
		glog.Info(msg)
		messenger.SendMessage(msg)
		rt := NewRecordTester(crt.ctx, crt.lapi, crt.lanalyzers, true, crt.useHTTP, crt.mp4, crt.streamHealth)
		es, err := rt.Start(fileName, testDuration, pauseDuration)
		if err == context.Canceled {
			msg := fmt.Sprintf("Test of %s cancelled", crt.host)
			messenger.SendMessage(msg)
			return err
		} else if err != nil || es != 0 {
			var re *testers.RTMPError
			if errors.As(err, &re) && try < 4 {
				msg := fmt.Sprintf(":rotating_light: Test of %s ended with RTMP err=%v errCode=%v try=%d, trying %s time",
					crt.host, err, es, try, getNth(try+2))
				messenger.SendMessage(msg)
				rt.Clean()
				try++
				time.Sleep(10 * time.Second)
				continue
			}
			if notRtmpTry < 3 {
				msg := fmt.Sprintf(":rotating_light: Test of %s ended with some err=%v errCode=%v try=%d, trying %s time",
					crt.host, err, es, notRtmpTry, getNth(notRtmpTry+2))
				messenger.SendMessage(msg)
				rt.Clean()
				notRtmpTry++
				time.Sleep(5 * time.Second)
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
						Summary:   fmt.Sprintf("Record tester :movie_camera: for %s error: %v", crt.host, err),
					},
				}
				sid := rt.StreamID()
				if sid != "" {
					link := pagerDutyLink{
						Href: "https://livepeer.com/dashboard/streams/" + sid,
						Text: "Stream",
					}
					event.Links = append(event.Links, link)
					stream := rt.Stream()
					if stream != nil {
						link = pagerDutyLink{
							Href: "https://my.papertrailapp.com/events?q=" + stream.ID + "+OR+" + stream.StreamKey + "+OR+" + stream.PlaybackID,
							Text: "Papertrail",
						}
						event.Links = append(event.Links, link)
					}
				}
				resp, err := pagerduty.ManageEvent(event)
				if err != nil {
					glog.Error(fmt.Errorf("PAGERDUTY Error: %w", err))
					messenger.SendFatalMessage(fmt.Sprintf("Error creating PagerDuty event: %v", err))
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
		notRtmpTry = 0
		rt.Clean()
		glog.Infof("Waiting %s before next test", pauseBetweenTests)
		select {
		case <-crt.ctx.Done():
			return context.Canceled
		case <-time.After(pauseBetweenTests):
		}
	}
}

func (crt *continuousRecordTester) Cancel() {
	crt.cancel()
}

func (crt *continuousRecordTester) Done() <-chan struct{} {
	return crt.ctx.Done()
}

var nth = []string{"0", "first", "second", "third", "forth", "fifth"}

func getNth(i int) string {
	if i > 0 && i < len(nth) {
		return nth[i]
	}
	return strconv.Itoa(i)
}
