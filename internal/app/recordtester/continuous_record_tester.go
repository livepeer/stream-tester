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

	ContinuousRecordTesterOptions struct {
		PagerDutyIntegrationKey string
		PagerDutyComponent      string
		PagerDutyLowUrgency     bool
		RecordTesterOptions
	}

	continuousRecordTester struct {
		ctx                     context.Context
		cancel                  context.CancelFunc
		host                    string // API host being tested
		pagerDutyIntegrationKey string
		pagerDutyComponent      string
		pagerDutyLowUrgency     bool
		rtOpts                  RecordTesterOptions
	}

	pagerDutyLink struct {
		Href string `json:"href,omitempty"`
		Text string `json:"text,omitempty"`
	}
)

// NewContinuousRecordTester returns new object
func NewContinuousRecordTester(gctx context.Context, opts ContinuousRecordTesterOptions) IContinuousRecordTester {
	ctx, cancel := context.WithCancel(gctx)
	server := opts.Client.GetServer()
	u, _ := url.Parse(server)
	crt := &continuousRecordTester{
		ctx:                     ctx,
		cancel:                  cancel,
		host:                    u.Host,
		pagerDutyIntegrationKey: opts.PagerDutyIntegrationKey,
		pagerDutyComponent:      opts.PagerDutyComponent,
		pagerDutyLowUrgency:     opts.PagerDutyLowUrgency,
		rtOpts:                  opts.RecordTesterOptions,
	}
	return crt
}

func (crt *continuousRecordTester) Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error {
	messenger.SendMessage(fmt.Sprintf("Starting continuous test of %s", crt.host))
	try := 0
	notRtmpTry := 0
	maxTestDuration := 2*testDuration + pauseDuration + 15*time.Minute
	for {
		msg := fmt.Sprintf(":arrow_right: Starting %s recordings test stream to %s", 2*testDuration, crt.host)
		messenger.SendMessage(msg)

		ctx, cancel := context.WithTimeout(crt.ctx, maxTestDuration)
		rt := NewRecordTester(ctx, crt.rtOpts)
		es, err := rt.Start(fileName, testDuration, pauseDuration)
		rt.Clean()
		ctxErr := ctx.Err()
		cancel()

		if crt.ctx.Err() != nil {
			messenger.SendMessage(fmt.Sprintf("Continuous record test of %s cancelled", crt.host))
			return crt.ctx.Err()
		} else if ctxErr != nil {
			msg := fmt.Sprintf("Record test of %s timed out, potential deadlock! ctxErr=%q err=%q", crt.host, ctxErr, err)
			messenger.SendFatalMessage(msg)
		} else if err != nil || es != 0 {
			var re *testers.RTMPError
			if errors.As(err, &re) && try < 4 {
				msg := fmt.Sprintf(":rotating_light: Test of %s ended with RTMP err=%v errCode=%v try=%d, trying %s time",
					crt.host, err, es, try, getNth(try+2))
				messenger.SendMessage(msg)
				try++
				time.Sleep(10 * time.Second)
				continue
			}
			if notRtmpTry < 3 {
				msg := fmt.Sprintf(":rotating_light: Test of %s ended with some err=%v errCode=%v try=%d, trying %s time",
					crt.host, err, es, notRtmpTry, getNth(notRtmpTry+2))
				messenger.SendMessage(msg)
				notRtmpTry++
				time.Sleep(5 * time.Second)
				continue
			}
			msg := fmt.Sprintf(":rotating_light: Test of %s ended with err=%v errCode=%v", crt.host, err, es)
			messenger.SendFatalMessage(msg)
			glog.Warning(msg)
			crt.sendPagerdutyEvent(rt, err)
		} else {
			msg := fmt.Sprintf(":white_check_mark: Test of %s succeeded", crt.host)
			messenger.SendMessage(msg)
			glog.Info(msg)
			crt.sendPagerdutyEvent(rt, nil)
		}
		try = 0
		notRtmpTry = 0
		glog.Infof("Waiting %s before next test", pauseBetweenTests)
		select {
		case <-crt.ctx.Done():
			messenger.SendMessage(fmt.Sprintf("Continuous record test of %s cancelled", crt.host))
			return err
		case <-time.After(pauseBetweenTests):
		}
	}
}

func (crt *continuousRecordTester) sendPagerdutyEvent(rt IRecordTester, err error) {
	if crt.pagerDutyIntegrationKey == "" {
		return
	}
	severity, lopriPrefix, dedupKey := "error", "", fmt.Sprintf("cont-record-tester:%s", crt.host)
	if crt.pagerDutyLowUrgency {
		severity, lopriPrefix = "warning", "[LOPRI] "
		dedupKey = "lopri-" + dedupKey
	}
	event := pagerduty.V2Event{
		RoutingKey: crt.pagerDutyIntegrationKey,
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
		Source:    crt.host,
		Component: crt.pagerDutyComponent,
		Severity:  severity,
		Summary:   fmt.Sprintf("%s:movie_camera: %s for `%s` error: %v", lopriPrefix, crt.pagerDutyComponent, crt.host, err),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
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
