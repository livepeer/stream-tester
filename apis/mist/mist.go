// Package mist API
package mist

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/model"
)

const httpTimeout = 2 * time.Second

var httpClient = &http.Client{
	// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	// Transport: &http2.Transport{AllowHTTP: true},
	// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, AllowHTTP: true},
	Timeout: httpTimeout,
}

var (
	P720p60fps16x9 = Profile{Name: "P720p60fps16x9", Bitrate: 6000000, Fps: 60, Width: 1280, Height: 720}
	P720p30fps16x9 = Profile{Name: "P720p30fps16x9", Bitrate: 4000000, Fps: 30, Width: 1280, Height: 720}
	P720p25fps16x9 = Profile{Name: "P720p25fps16x9", Bitrate: 3500000, Fps: 25, Width: 1280, Height: 720}
	P720p30fps4x3  = Profile{Name: "P720p30fps4x3", Bitrate: 3500000, Fps: 30, Width: 960, Height: 720}
	P576p30fps16x9 = Profile{Name: "P576p30fps16x9", Bitrate: 1500000, Fps: 30, Width: 1024, Height: 576}
	P576p25fps16x9 = Profile{Name: "P576p25fps16x9", Bitrate: 1500000, Fps: 25, Width: 1024, Height: 576}
	P360p30fps16x9 = Profile{Name: "P360p30fps16x9", Bitrate: 1200000, Fps: 30, Width: 640, Height: 360}
	P360p25fps16x9 = Profile{Name: "P360p25fps16x9", Bitrate: 1000000, Fps: 25, Width: 640, Height: 360}
	P360p30fps4x3  = Profile{Name: "P360p30fps4x3", Bitrate: 1000000, Fps: 30, Width: 480, Height: 360}
	P240p30fps16x9 = Profile{Name: "P240p30fps16x9", Bitrate: 600000, Fps: 30, Width: 426, Height: 240}
	P240p25fps16x9 = Profile{Name: "P240p25fps16x9", Bitrate: 600000, Fps: 25, Width: 426, Height: 240}
	P240p30fps4x3  = Profile{Name: "P240p30fps4x3", Bitrate: 600000, Fps: 30, Width: 320, Height: 240}
	P144p30fps16x9 = Profile{Name: "P144p30fps16x9", Bitrate: 400000, Fps: 30, Width: 256, Height: 144}
	P144p25fps16x9 = Profile{Name: "P144p25fps16x9", Bitrate: 400000, Fps: 25, Width: 256, Height: 144}
)

// ProfileLookup presets
var ProfileLookup = map[string]Profile{
	"P720p60fps16x9": P720p60fps16x9,
	"P720p30fps16x9": P720p30fps16x9,
	"P720p25fps16x9": P720p25fps16x9,
	"P720p30fps4x3":  P720p30fps4x3,
	"P576p30fps16x9": P576p30fps16x9,
	"P576p25fps16x9": P576p25fps16x9,
	"P360p30fps16x9": P360p30fps16x9,
	"P360p25fps16x9": P360p25fps16x9,
	"P360p30fps4x3":  P360p30fps4x3,
	"P240p30fps16x9": P240p30fps16x9,
	"P240p25fps16x9": P240p25fps16x9,
	"P240p30fps4x3":  P240p30fps4x3,
	"P144p30fps16x9": P144p30fps16x9,
}

type (

	// API object incapsulating Mist's server API
	API struct {
		host              string
		apiURL            string
		apiURLVerbose     string
		login             string
		password          string
		livepeerToken     string
		challenge         string // value of challenge field sent by Mist
		challengeRepsonse string
	}

	MistResp struct {
		Authorize     *authorize         `json:"authorize,omitempty"`
		ActiveStreams []string           `json:"active_streams,omitempty"`
		Streams       map[string]*Stream `json:"streams,omitempty"`
		Config        *Config            `json:"config,omitempty"`
	}

	Config struct {
		Accesslog  string `json:"accesslog,omitempty"`
		Controller struct {
			Interface interface{} `json:"interface,omitempty"`
			Port      interface{} `json:"port,omitempty"`
			Username  interface{} `json:"username,omitempty"`
		} `json:"controller,omitempty"`
		Debug      interface{} `json:"debug,omitempty"`
		Prometheus string      `json:"prometheus,omitempty"`
		Protocols  []struct {
			Connector  string      `json:"connector,omitempty"`
			Nonchunked bool        `json:"nonchunked,omitempty"`
			Online     interface{} `json:"online,omitempty"`
			Pubaddr    string      `json:"pubaddr,omitempty"`
		} `json:"protocols,omitempty"`
		Time     int64       `json:"time,omitempty"`
		Version  string      `json:"version,omitempty"`
		Triggers TriggersMap `json:"triggers,omitempty"`
	}

	TriggersMap map[string][]Trigger

	Trigger struct {
		Default string   `json:"default,omitempty"`
		Handler string   `json:"handler"`
		Streams []string `json:"streams"`
		Sync    bool     `json:"sync"`
		Params  string   `json:"params,omitempty"`
	}

	Profile struct {
		Bitrate     int    `json:"bitrate"` // 4000000
		Fps         int    `json:"fps"`
		Width       int    `json:"width"`
		Height      int    `json:"height"`
		Name        string `json:"name"`
		HumanName   string `json:"x-LSP-name"`
		CustomField string `json:"custom_field,omitempty"`
	}

	Process struct {
		AccessToken    string    `json:"access_token,omitempty"`
		Process        string    `json:"process,omitempty"`
		TargetProfiles []Profile `json:"target_profiles,omitempty"`
		HumanName      string    `json:"x-LSP-name,omitempty"`
		Leastlive      string    `json:"leastlive,omitempty"`
		CustomURL      string    `json:"custom_url,omitempty"`
		AudioSelect    string    `json:"audio_select,omitempty"`
	}

	Stream struct {
		Name         string     `json:"name,omitempty"`
		Online       int        `json:"online,omitempty"`
		Source       string     `json:"source,omitempty"`
		Segmentsize  string     `json:"segmentsize,omitempty"` // ms
		StopSessions bool       `json:"stop_sessions,omitempty"`
		Realtime     bool       `json:"realtime,omitempty"`
		Processes    []*Process `json:"processes,omitempty"`
	}

	authorize struct {
		Challenge string `json:"challenge,omitempty"`
		Status    string `json:"status,omitempty"`
	}

	authResp struct {
		Authorize authorize `json:"authorize,omitempty"`
		Streams   map[string]*Stream
	}

	authReq struct {
		Username string `json:"username,omitempty"`
		Password string `json:"password,omitempty"`
	}

	addStreamReq struct {
		Authorize *authReq           `json:"authorize,omitempty"`
		Minimal   int                `json:"minimal"`
		Addstream map[string]*Stream `json:"addstream,omitempty"`
	}

	deleteStreamReq struct {
		Authorize    *authReq `json:"authorize,omitempty"`
		Minimal      int      `json:"minimal,omitempty"`
		Deletestream []string `json:"deletestream,omitempty"`
	}
)

// NewMist creates new MistAPI object
func NewMist(host, login, password, livepeerToken string, port uint) *API {
	url := fmt.Sprintf("http://%s:%d/api2", host, port)
	urlv := fmt.Sprintf("http://%s:%d/api", host, port)
	return &API{
		host:          host,
		login:         login,
		password:      password,
		apiURL:        url,
		apiURLVerbose: urlv,
		livepeerToken: livepeerToken,
	}
}

// Login authenicates to the Mist server
func (mapi *API) Login() error {
	// u := mapi.apiURL + "?" + url.QueryEscape()
	resp, err := httpClient.Do(uhttp.GetRequest(mapi.apiURL))
	if err != nil {
		glog.Errorf("Error authenticating to Mist server (%s) error: %v", mapi.apiURL, err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Errorf("===== status error contacting Mist server (%s) status %d body: %s", mapi.apiURL, resp.StatusCode, string(b))
		return fmt.Errorf("Status error %s", resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error authenticating to Mist server (%s) error: %v", mapi.apiURL, err)
		return err
	}
	glog.Info(string(b))
	auth := &authResp{}
	err = json.Unmarshal(b, auth)
	if err != nil {
		panic(err)
	}
	glog.Infof("challenge: %s, status: %s", auth.Authorize.Challenge, auth.Authorize.Status)
	if auth.Authorize.Status != "CHALL" {
		if auth.Authorize.Status == "OK" {
			return nil
		}
		return fmt.Errorf("Unexpected status: %s", auth.Authorize.Status)
	}

	mapi.challenge = auth.Authorize.Challenge
	mapi.challengeRepsonse = mapi.caclChallengeResponse(mapi.password, mapi.challenge)

	u := mapi.apiURL + "?minimal=0&command=" + url.QueryEscape(fmt.Sprintf(`{"authorize":{"username":"%s","password":"%s"}}`, mapi.login, mapi.challengeRepsonse))
	resp, err = httpClient.Do(uhttp.GetRequest(u))
	if err != nil {
		glog.Errorf("Error authenticating to Mist server (%s) error: %v", u, err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Errorf("===== status error contacting Mist server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		return fmt.Errorf("Status error %s", resp.Status)
	}
	b, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		glog.Errorf("Error authenticating to Mist server (%s) error: %v", mapi.apiURL, err)
		return err
	}
	glog.Info(string(b))
	return nil
}

func (mapi *API) caclChallengeResponse(password, challenge string) string {
	mh := md5.New()
	h1 := md5.Sum([]byte(password))
	mh.Write([]byte(hex.EncodeToString(h1[:])))
	mh.Write([]byte(challenge))
	hashRes := hex.EncodeToString(mh.Sum(nil))
	glog.Infof("Challenge response: %s", hashRes)
	return hashRes
}

// CreateStream creates new stream in Mist server
// func (mapi *API) CreateStream(name string, presets []string, profiles []Profile, segmentSize, customURL, source string) error
func (mapi *API) CreateStream(name string, presets []string, profiles []Profile, segmentSize, customURL, source string, skipTranscoding, sendAudio bool) error {
	glog.Infof("Creating Mist stream '%s' with presets '%+v' profiles %+v", name, presets, profiles)
	reqs := &addStreamReq{
		Minimal:   1,
		Addstream: make(map[string]*Stream),
		Authorize: &authReq{
			Username: mapi.login,
			Password: mapi.challengeRepsonse,
		},
	}
	var targetProfiles []Profile
	if len(profiles) > 0 {
		targetProfiles = profiles
	} else if len(presets) > 0 {
		targetProfiles = Presets2Profiles(presets)
	}
	if source == "" {
		source = "push://"
	}
	reqs.Addstream[name] = &Stream{
		Name:        name,
		Source:      source,
		Segmentsize: segmentSize,
		// Processes:   []*Process{{Process: "Livepeer", AccessToken: mapi.livepeerToken, TargetProfiles: targetProfiles, Leastlive: "1", CustomURL: customURL}},
	}
	if !skipTranscoding {
		audioSelect := ""
		if sendAudio {
			audioSelect = "maxbps"
		}
		reqs.Addstream[name].Processes = []*Process{{
			Process:        "Livepeer",
			AccessToken:    mapi.livepeerToken,
			TargetProfiles: targetProfiles,
			Leastlive:      "1",
			CustomURL:      customURL,
			AudioSelect:    audioSelect,
		}}
	}

	_, err := mapi.post(reqs, false)
	return err
}

// DeleteStreams removes streams from Mist server
func (mapi *API) DeleteStreams(names ...string) error {
	glog.Infof("Deleting Mist streams '%v'", names)
	reqs := &deleteStreamReq{
		Authorize: &authReq{
			Username: mapi.login,
			Password: mapi.challengeRepsonse,
		},
		Minimal:      1,
		Deletestream: make([]string, 0),
	}
	for _, s := range names {
		reqs.Deletestream = append(reqs.Deletestream, s)
	}
	_, err := mapi.post(reqs, false)
	return err
}

// Streams gets list of all streams
func (mapi *API) Streams() (map[string]*Stream, []string, error) {
	u := mapi.apiURLVerbose + "?minimal=0&command=" + url.QueryEscape(fmt.Sprintf(`{"active_streams":1,"authorize":{"username":"%s","password":"%s"}}`, mapi.login, mapi.challengeRepsonse))
	resp, err := httpClient.Do(uhttp.GetRequest(u))
	if err != nil {
		glog.Errorf("Error authenticating to Mist server (%s) error: %v", u, err)
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Errorf("===== status error contacting Mist server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		return nil, nil, fmt.Errorf("Status error %s", resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		glog.Errorf("Error authenticating to Mist server (%s) error: %v", mapi.apiURL, err)
		return nil, nil, err
	}
	glog.V(model.VVERBOSE).Info(string(b))
	mr := &MistResp{}
	err = json.Unmarshal(b, mr)
	if err != nil {
		return nil, nil, err
	}
	return mr.Streams, mr.ActiveStreams, nil
}

// GetTriggers returns map of triggers
func (mapi *API) GetTriggers() (TriggersMap, error) {
	config, err := mapi.GetConfig()
	if err != nil {
		return nil, err
	}
	glog.Infof("Got config: %+v", config)

	return config.Triggers, nil
}

// SetTriggers sets triggers
func (mapi *API) SetTriggers(triggers TriggersMap) error {
	mr := &MistResp{
		Config: &Config{
			Triggers: triggers,
		},
	}
	_, err := mapi.post(mr, false)
	return err
}

// GetConfig returns config
func (mapi *API) GetConfig() (*Config, error) {
	u := mapi.apiURL + "?minimal=0&command=" + url.QueryEscape(fmt.Sprintf(`{"config":{},"authorize":{"username":"%s","password":"%s"}}`, mapi.login, mapi.challengeRepsonse))
	resp, err := httpClient.Do(uhttp.GetRequest(u))
	if err != nil {
		glog.Errorf("Error authenticating to Mist server (%s) error: %v", u, err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Errorf("===== status error contacting Mist server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		return nil, fmt.Errorf("Status error %s", resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		glog.Errorf("Error getting config from Mist server (%s) error: %v", mapi.apiURL, err)
		return nil, err
	}
	glog.V(model.VERBOSE).Info(string(b))
	mr := &MistResp{}
	if err = json.Unmarshal(b, mr); err != nil {
		return nil, err
	}
	return mr.Config, nil
}

func (mapi *API) post(commandi interface{}, verbose bool) ([]byte, error) {
	var resp *http.Response
	cmdb, err := json.Marshal(commandi)
	if err != nil {
		return nil, err
	}
	command := string(cmdb)
	glog.Infof("Sending request %s", command)
	params := url.Values{}
	params.Add("command", command)
	body := strings.NewReader(params.Encode())
	// req := uhttp.RequireRequest(mapi.apiURL, "application/json", body)
	uri := mapi.apiURL
	if verbose {
		uri = mapi.apiURLVerbose
	}
	req := uhttp.RequireRequest("POST", uri, body)
	req.Header.Add("Content-Type", "application/json")
	try := 0
	for try < 3 {
		resp, err = httpClient.Do(req)
		if err != nil {
			glog.Infof("Error sending Mist command=%v err=%v, retrying", commandi, err)
			try++
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			glog.Infof("Status error sending Mist command status %d, retrying", resp.StatusCode)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		glog.Infof("Error sending Mist command=%v err=%v", commandi, err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Status error sending Mist command status %d", resp.StatusCode)
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("Error sending Mist command %v", err)
	}
	glog.Info("Mist response: " + string(b))

	auth := &authResp{}
	err = json.Unmarshal(b, auth)
	if err != nil {
		return b, nil
	}
	glog.Infof("challenge: %s, status: %s", auth.Authorize.Challenge, auth.Authorize.Status)
	if auth.Authorize.Status == "CHALL" {
		// need to re-authenticate
		if auth.Authorize.Challenge == mapi.challenge {
			glog.Errorf("Error authenticating to Mist")
		} else {
			mapi.challenge = auth.Authorize.Challenge
			mapi.challengeRepsonse = mapi.caclChallengeResponse(mapi.password, mapi.challenge)
			return mapi.post(commandi, verbose)
		}
	}
	return b, nil
}

func (st *Stream) String() string {
	r := fmt.Sprintf("Name: %s\nOnline: %d\nSource: %s\n", st.Name, st.Online, st.Source)
	if st.Segmentsize != "" {
		r += fmt.Sprintf("Segment size: %sms\n", st.Segmentsize)
	}
	for _, pro := range st.Processes {
		r += fmt.Sprintf("  Process %s type %s profiles %+v token %s leastlive %s\n",
			pro.HumanName, pro.Process, pro.TargetProfiles, pro.AccessToken, pro.Leastlive)
	}
	return r
}

// PresetsStr2Profiles takes comma-separated presets list and returns according profiles
func PresetsStr2Profiles(presets string) []Profile {
	return Presets2Profiles(strings.Split(presets, ","))
}

// Presets2Profiles takes comma-separated presets list and returns according profiles
func Presets2Profiles(presets []string) []Profile {
	var res []Profile
	for _, preset := range presets {
		if profile, has := ProfileLookup[preset]; has {
			res = append(res, profile)
		}
	}
	return res
}
