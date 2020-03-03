package testdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

// Tester maintains each individual test run
type Tester struct {
	httpClient *http.Client
	host       string
	port       uint16
	// rtmpHost   string
	// rtmpPort   uint16
	// mediaHost  string
	// mediaPort  uint16

	checkInterval  time.Duration
	minSuccessRate float64
	numStreamsInit uint
	numStreamsStep uint

	baseManifestID string
	done           bool
	streamConfig   *model.StartStreamsReq

	alert func(s string, r *Result)
}

// NewTester creates a new Tester
// func NewTester(httpClient *http.Client, host, rtmpHost, mediaHost string, port, rtmpPort, mediaPort uint, checkInterval time.Duration, minSuccessRate float64, numStreamsInit, numStreamsStep uint, alert func(string, *Result)) *Tester {
func NewTester(httpClient *http.Client, host string, port uint, checkInterval time.Duration, minSuccessRate float64,
	numStreamsInit, numStreamsStep uint, streamConfig *model.StartStreamsReq, alert func(string, *Result)) *Tester {
	t := &Tester{
		httpClient:     httpClient,
		host:           host,
		port:           uint16(port),
		checkInterval:  checkInterval,
		minSuccessRate: minSuccessRate,
		numStreamsInit: numStreamsInit,
		numStreamsStep: numStreamsStep,
		alert:          alert,
		streamConfig:   streamConfig,
		// rtmpHost:       rtmpHost,
		// rtmpPort:       uint16(rtmpPort),
		// mediaHost:      mediaHost,
		// mediaPort:      uint16(mediaPort),
	}
	return t
}

// IsRunning returns true if the benchmark is running, and false otherwise
func (t *Tester) IsRunning() bool { return t.baseManifestID != "" && !t.done }

// GetManifestID returns the manifest ID of the stream in progress
func (t *Tester) GetManifestID() string { return t.baseManifestID }

func (t *Tester) start(numStreams, numProfiles uint) (*model.StartStreamsRes, error) {
	response := &model.StartStreamsRes{}

	url := fmt.Sprintf("http://%s:%d/start_streams", t.host, t.port)
	contentType := "application/json"

	/*
		startStreamReq := model.StartStreamsReq{
			Host:           t.rtmpHost,
			MHost:          t.mediaHost,
			FileName:       "official_test_source_2s_keys_24pfs.mp4",
			RTMP:           t.rtmpPort,
			Media:          t.mediaPort,
			Repeat:         1,
			Simultaneous:   numStreams,
			ProfilesNum:    int(numProfiles),
			MeasureLatency: false,
			HTTPIngest:     false,
		}
	*/
	startStreamReq := *t.streamConfig
	startStreamReq.Simultaneous = numStreams

	startStreamReqBytes, err := json.Marshal(startStreamReq)
	if err != nil {
		glog.Errorf("json.Marshall failed: %v", err)
	}
	postBody := strings.NewReader(string(startStreamReqBytes))

	glog.Infof("Sending request %+v", startStreamReq)
	resp, err := t.httpClient.Post(url, contentType, postBody)
	if err != nil {
		glog.Errorf("start() failed: %v", err)
		return response, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("ReadAll failed: %v", err)
		return response, err
	}

	err = json.Unmarshal(body, response)
	if err != nil {
		glog.Errorf("Unmarshal failed: %v", err)
		glog.Infof("body: %s", string(body))
		return response, err
	}

	t.baseManifestID = response.BaseManifestID

	glog.Infof("start: %v", string(body))

	return response, nil
}

func (t *Tester) stop() error {
	url := fmt.Sprintf("http://%s:%d/stop", t.host, t.port)
	_, err := t.httpClient.Get(url)
	if err != nil {
		glog.Errorf("stop() failed: %v", err)
	}
	return err
}

func (t *Tester) stats() (*model.Stats, error) {
	stats := &model.Stats{}
	url := fmt.Sprintf("http://%s:%d/stats?latencies&base_manifest_id=%s", t.host, t.port, t.baseManifestID)

	resp, err := t.httpClient.Get(url)
	if err != nil {
		glog.Errorf("stats() failed: %v", err)
		return stats, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("ReadAll failed: %v", err)
		return stats, err
	}
	if resp.StatusCode != http.StatusOK {
		glog.Errorf("Unmarshal failed: err=%v body=%s", err, string(body))
		if resp.StatusCode == http.StatusNotFound {
			return stats, model.ErroNotFound
		}
		return stats, fmt.Errorf("Status error getting stats: code=%s msg=%s", resp.Status, string(body))
	}

	err = json.Unmarshal(body, stats)
	if err != nil {
		glog.Errorf("Unmarshal failed: err=%v body=%s", err, string(body))
		return stats, err
	}

	glog.V(model.VVERBOSE).Infof("stats: %v", string(body))

	return stats, nil
}

// Result contains the results of the Run() function
type Result struct {
	NumStreams,
	NumProfiles uint
	Stats *model.Stats
}

// Run executes the stream concurrency benchmark and returns the Result
func (t *Tester) Run(ctx context.Context, NumProfiles uint) (*Result, error) {
	numStreams := t.numStreamsInit
	t.done = false

	stats := &model.Stats{}

	started := false
	for !started || stats.SuccessRate > t.minSuccessRate {

		started = true

		t.alert("begin iteration: ", &Result{numStreams, NumProfiles, stats})

		select {
		case <-ctx.Done():
			return &Result{}, fmt.Errorf("context cancelled")
		default:
		}

		startStreamResponse, err := t.start(numStreams, NumProfiles)
		if err != nil {
			return &Result{}, fmt.Errorf("start stream failed: %v", err)
		}
		if !startStreamResponse.Success {
			return &Result{}, fmt.Errorf("start stream failed: success = %v", startStreamResponse.Success)
		}

		c := time.NewTicker(t.checkInterval)

	loop:
		for {
			select {
			case <-ctx.Done():
				return &Result{}, fmt.Errorf("context cancelled")
			case <-c.C:
				stats, err = t.stats()
				if err != nil {
					// return &Result{}, fmt.Errorf("get stats failed: %v", err)
					if err == model.ErroNotFound {
						messenger.SendFatalMessage(fmt.Sprintf("Stream %s not found, something wrong, exiting.", t.baseManifestID))
						return &Result{numStreams, NumProfiles, stats}, nil
					}
					messenger.SendMessage(fmt.Sprintf("get stats failed: %v", err))
					continue
				}
				emsg := fmt.Sprintf("Running %d streams: %s", numStreams, stats.FormatForConsole())
				glog.V(model.VERBOSE).Infoln(emsg)
				messenger.SendMessage(emsg)

				if stats.Finished {
					break loop
				}
			}
		}

		emsg := fmt.Sprintf("Result of running %d streams:\n %s", numStreams, stats.FormatForConsole())
		glog.Infoln(emsg)
		messenger.SendCodeMessage(emsg)
		if emsg := stats.FormatErrorsForConsole(); emsg != "" {
			glog.Infoln(emsg)
			messenger.SendCodeMessage(emsg)
		}
		numStreams += t.numStreamsStep
		// wait before starting new round, allow streams in B/O to timeout
		if model.Production {
			time.Sleep(61 * time.Second)
		}
	}
	t.done = true
	msg := fmt.Sprintf("Success degrades at %d streams with %d profiles: %v", numStreams, NumProfiles, stats.SuccessRate)
	glog.Infoln(msg)
	messenger.SendMessage(msg)
	glog.Infof("Stats from running %d streams: %s", numStreams, stats.FormatForConsole())
	if emsg := stats.FormatErrorsForConsole(); emsg != "" {
		glog.Infoln(emsg)
	}
	return &Result{numStreams, NumProfiles, stats}, nil
}
