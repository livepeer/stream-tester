package recordtester

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PagerDuty/go-pagerduty"
	"github.com/google/uuid"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/messenger"
)

type (
	// IContinuousRecordTester ...
	IContinuousRecordTester interface {
		// Start start test. Blocks until error.
		Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error
		// Setup
		Setup() error
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
		useHTTP                 bool
		mp4                     bool
		testWebhooks            bool
		webhooksExternalUrl     string
		webhooksExternalUrlFull string
		webhookBind             string
		region                  string
		mu                      sync.Mutex
		webhookCalls            []*webhookCall
		webhooksToTest          []string
		webhookID               string
		skipUnknownHooks        bool
	}

	webhookCall struct {
		Body    string
		Payload *hookPayload
		Method  string
	}

	hookPayload struct {
		ID        string                     `json:"id,omitempty"`
		WebhookId string                     `json:"webhookId,omitempty"`
		CreatedAt int64                      `json:"createdAt,omitempty"`
		Timestamp int64                      `json:"timestamp,omitempty"`
		Event     string                     `json:"event,omitempty"`
		Stream    *livepeer.CreateStreamResp `json:"stream,omitempty"`
		Payload   map[string]interface{}
	}

	pagerDutyLink struct {
		Href string `json:"href,omitempty"`
		Text string `json:"text,omitempty"`
	}
)

// NewContinuousRecordTester returns new object
func NewContinuousRecordTester(gctx context.Context, lapi *livepeer.API, pagerDutyIntegrationKey, pagerDutyComponent string,
	useHTTP, mp4 bool, whtBind, whtExternalUrl, region string) IContinuousRecordTester {

	ctx, cancel := context.WithCancel(gctx)
	server := lapi.GetServer()
	u, _ := url.Parse(server)
	var webhooksExternalUrlFull string
	var webhooksToTest []string
	if whtExternalUrl != "" {
		wu, err := url.Parse(whtExternalUrl)
		if err != nil {
			panic(err)
		}
		wu.Path = "/webhook"
		webhooksExternalUrlFull = wu.String()
		webhooksToTest = []string{"stream.started", "stream.idle"}
	}
	crt := &continuousRecordTester{
		lapi:                    lapi,
		ctx:                     ctx,
		cancel:                  cancel,
		host:                    u.Host,
		pagerDutyIntegrationKey: pagerDutyIntegrationKey,
		pagerDutyComponent:      pagerDutyComponent,
		useHTTP:                 useHTTP,
		mp4:                     mp4,
		testWebhooks:            whtExternalUrl != "",
		webhooksExternalUrl:     whtExternalUrl,
		webhooksExternalUrlFull: webhooksExternalUrlFull,
		webhookBind:             whtBind,
		webhooksToTest:          webhooksToTest,
		region:                  region,
	}
	return crt
}

func (crt *continuousRecordTester) Setup() error {
	if !crt.testWebhooks {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", crt.handleHook)

	srv := &http.Server{
		Addr:    crt.webhookBind,
		Handler: mux,
	}

	go func() {
		<-crt.ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		glog.Infof("Shutting down webhook server")
		srv.Shutdown(c)
	}()

	go func() {
		glog.Info("Webhook server listening on ", crt.webhookBind)
		err := srv.ListenAndServe()
		if err != http.ErrServerClosed {
			glog.Errorf("Error in listen err=%v", err)
			if strings.Contains(err.Error(), "bind:") {
				os.Exit(21)
			}
		}
	}()
	time.Sleep(10 * time.Millisecond)
	// try to access webhook
	callID := "testCall-" + uuid.NewString()
	if err := crt.makeTestCall(callID); err != nil {
		return err
	}

	for {
		firstCall := crt.getFirstHookCall()
		if firstCall.Body == "" {
			return errors.New("can't access webhook server by external URL")
		}
		if firstCall.Body == callID {
			break
		}
	}
	if err := crt.ensureHook(); err != nil {
		return err
	}
	crt.skipUnknownHooks = true

	return nil
}

func (crt *continuousRecordTester) ensureHook() error {
	hooks, err := crt.lapi.GetWebhooksR()
	if err != nil {
		return err
	}
	glog.Infof("got %+v", hooks)
	var hookID string
	var hooksIDsToDel []string
	for _, hook := range hooks {
		if hook.URL == crt.webhooksExternalUrlFull {
			if hookID == "" && utils.StringsArraysEq(hook.Events, crt.webhooksToTest) {
				hookID = hook.ID
			} else {
				hooksIDsToDel = append(hooksIDsToDel, hook.ID)
			}
		}
	}
	for _, hid := range hooksIDsToDel {
		err = crt.lapi.DeleteWebhookR(hid)
		if err != nil {
			return err
		}

	}
	if hookID == "" {
		// creating hook
		name := "record-tester"
		if crt.region != "" {
			name = crt.region + "-" + name
		}
		hook, err := crt.lapi.CreateWebhook(name, crt.webhooksExternalUrlFull, crt.webhooksToTest)
		if err != nil {
			return err
		}
		glog.Infof("Using webhook with name=%q id=%s", hook.Name, hookID)
		crt.webhookID = hook.ID
	} else {
		glog.Infof("Using webhook id=%s", hookID)
		crt.webhookID = hookID
	}

	return nil
}

const httpTimeout = 4 * time.Second

func (crt *continuousRecordTester) makeTestCall(callID string) error {

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	req := uhttp.NewRequestWithContext(ctx, "POST", crt.webhooksExternalUrlFull, bytes.NewBuffer([]byte(callID)))
	resp, err := http.DefaultClient.Do(req)
	cancel()
	if err != nil {
		glog.Errorf("Error making test call uri=%s err=%v", crt.webhooksExternalUrlFull, err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error making test call %s status %d body: %s", crt.webhooksExternalUrlFull, resp.StatusCode, string(b))
		return errors.New(http.StatusText(resp.StatusCode))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading from test call uri=%s err=%v", crt.webhooksExternalUrlFull, err)
		return err
	}
	val := string(b)
	glog.Infof("Read from test call url=%s: '%s'", crt.webhooksExternalUrlFull, val)
	return nil
}

func (crt *continuousRecordTester) getFirstHookCall() *webhookCall {
	var res *webhookCall
	crt.mu.Lock()
	if len(crt.webhookCalls) > 0 {
		res = crt.webhookCalls[0]
		crt.webhookCalls = crt.webhookCalls[1:]
	}
	crt.mu.Unlock()
	return res
}

func (crt *continuousRecordTester) clearWebhookCalls() {
	crt.mu.Lock()
	crt.webhookCalls = nil
	crt.mu.Unlock()
}

func (crt *continuousRecordTester) handleHook(w http.ResponseWriter, r *http.Request) {
	// glog.Infof("Got %s hook call from addr=%s UA=%s", r.Method, r.RemoteAddr, r.Header.Get("User-Agent"))
	body, err := io.ReadAll(r.Body)

	if crt.testWebhooks && err == nil && len(body) > 0 {
		hp := &hookPayload{}
		json.Unmarshal(body, hp)
		if crt.skipUnknownHooks && hp.WebhookId != "" && crt.webhookID != "" && crt.webhookID != hp.WebhookId {
			glog.Infof("----> skipping unknown webhook id %s", hp.WebhookId)
		} else {
			crt.mu.Lock()
			crt.webhookCalls = append(crt.webhookCalls, &webhookCall{
				Method:  r.Method,
				Body:    string(body),
				Payload: hp,
			})
			crt.mu.Unlock()
		}
	}
	glog.Infof("Got %s hook call from addr=%s UA=%s body=%s", r.Method, r.RemoteAddr, r.Header.Get("User-Agent"), body)
	w.WriteHeader(http.StatusOK)
}

func (crt *continuousRecordTester) Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error {
	glog.Infof("Starting continuous test of %s", crt.host)
	try := 0
	notRtmpTry := 0
	mult := 1
	if pauseDuration > 0 {
		mult = 2
	}
	for {
		crt.clearWebhookCalls()
		msg := fmt.Sprintf(":arrow_right: Starting %s recordings test stream to %s", 2*testDuration, crt.host)
		glog.Info(msg)
		messenger.SendMessage(msg)
		rt := NewRecordTester(crt.ctx, crt.lapi, true, crt.useHTTP, crt.mp4)
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
						Summary:   crt.host + ": " + err.Error(),
					},
				}
				sid := rt.StreamID()
				if sid != "" {
					link := pagerDutyLink{
						Href: "https://livepeer.com/app/stream/" + sid,
						Text: "Stream",
					}
					event.Links = append(event.Links, link)
					stream := rt.Stream()
					if stream != nil {
						plink := pagerDutyLink{
							Href: "https://my.papertrailapp.com/events?q=" + stream.ID + "+OR+" + stream.StreamKey + "+OR+" + stream.PlaybackID,
							Text: "Papertrail",
						}
						event.Links = append(event.Links, plink)
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
			good := true
			// check webhooks
			if crt.testWebhooks {
				sid := rt.StreamID()
				if sid == "" {
					panic("something bad happened!")
				}
				streamStarted := crt.countWebhookCalls(sid, "stream.started")
				streamIdle := crt.countWebhookCalls(sid, "stream.idle")
				var msgs []string
				if streamStarted != mult {
					msg := fmt.Sprintf("Expected %d 'stream.started' events, but got %d instead", mult, streamStarted)
					glog.Error(msg)
					msgs = append(msgs, msg)
					good = false
				}
				if streamIdle != mult {
					msg := fmt.Sprintf("Expected %d 'stream.idle' events, but got %d instead", mult, streamIdle)
					glog.Error(msg)
					msgs = append(msgs, msg)
					good = false
				}
				if !good {
					msg := fmt.Sprintf(":rotating_light: Test of %s ended, error in webhooks: %+v", crt.host, msgs)
					messenger.SendFatalMessage(msg)
					glog.Warning(msg)
				}
			}
			if good {
				msg := fmt.Sprintf(":white_check_mark: Test of %s succeed", crt.host)
				messenger.SendMessage(msg)
				glog.Warning(msg)
			}
		}
		try = 0
		notRtmpTry = 0
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

func (crt *continuousRecordTester) countWebhookCalls(streamID, event string) int {
	var res int
	crt.mu.Lock()
	for _, hc := range crt.webhookCalls {
		if hc.Payload.Stream != nil && hc.Payload.Stream.ID == streamID && hc.Payload.Event == event {
			res++
		}
	}
	crt.mu.Unlock()
	return res
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
