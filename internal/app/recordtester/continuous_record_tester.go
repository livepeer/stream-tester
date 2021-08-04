package recordtester

import (
	"bytes"
	"context"
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
		mu                      sync.Mutex
		webhookCalls            []string
	}

	pagerDutyLink struct {
		Href string `json:"href,omitempty"`
		Text string `json:"text,omitempty"`
	}
)

// NewContinuousRecordTester returns new object
func NewContinuousRecordTester(gctx context.Context, lapi *livepeer.API, pagerDutyIntegrationKey, pagerDutyComponent string,
	useHTTP, mp4 bool, whtBind, whtExternalUrl string) IContinuousRecordTester {

	ctx, cancel := context.WithCancel(gctx)
	server := lapi.GetServer()
	u, _ := url.Parse(server)
	var webhooksExternalUrlFull string
	if whtExternalUrl != "" {
		wu, err := url.Parse(whtExternalUrl)
		if err != nil {
			panic(err)
		}
		wu.Path = "/webhook"
		webhooksExternalUrlFull = wu.String()
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
		if firstCall == "" {
			return errors.New("can't access webhook server by external URL")
		}
		if firstCall == callID {
			break
		}
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

func (crt *continuousRecordTester) getFirstHookCall() string {
	var res string
	crt.mu.Lock()
	if len(crt.webhookCalls) > 0 {
		res = crt.webhookCalls[0]
		crt.webhookCalls = crt.webhookCalls[1:]
	}
	crt.mu.Unlock()
	return res
}

func (crt *continuousRecordTester) handleHook(w http.ResponseWriter, r *http.Request) {
	glog.Infof("Got %s hook call from %s/%s", r.Method, r.RemoteAddr, r.Header.Get("User-Agent"))
	body, err := io.ReadAll(r.Body)
	if err == nil && len(body) > 0 {
		crt.mu.Lock()
		crt.webhookCalls = append(crt.webhookCalls, string(body))
		crt.mu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
}

func (crt *continuousRecordTester) Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error {
	glog.Infof("Starting continuous test of %s", crt.host)
	try := 0
	notRtmpTry := 0
	for {
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
			msg := fmt.Sprintf(":white_check_mark: Test of %s succeed", crt.host)
			messenger.SendMessage(msg)
			glog.Warning(msg)
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
