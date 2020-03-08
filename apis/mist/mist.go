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

	authorize struct {
		Challenge string `json:"challenge,omitempty"`
		Status    string `json:"status,omitempty"`
	}

	authResp struct {
		Authorize authorize `json:"authorize,omitempty"`
		Streams   map[string]*Stream
	}

	Process struct {
		AccessToken   string `json:"access_token,omitempty"`
		Process       string `json:"process,omitempty"`
		TargetProfile string `json:"target_profile,omitempty"`
		HumanName     string `json:"x-LSP-name,omitempty"`
		Leastlive     string `json:"leastlive,omitempty"`
	}

	Stream struct {
		Name        string     `json:"name,omitempty"`
		Online      int        `json:"online,omitempty"`
		Source      string     `json:"source,omitempty"`
		Segmentsize string     `json:"segmentsize,omitempty"` // ms
		Processes   []*Process `json:"processes,omitempty"`
	}

	authReq struct {
		Username string `json:"username,omitempty"`
		Password string `json:"password,omitempty"`
	}

	addStreamReq struct {
		Minimal   int                `json:"minimal"`
		Addstream map[string]*Stream `json:"addstream,omitempty"`
	}

	deleteStreamReq struct {
		Authorize    *authReq `json:"authorize,omitempty"`
		Minimal      int      `json:"minimal,omitempty"`
		Deletestream []string `json:"deletestream,omitempty"`
	}

	MistResp struct {
		Authorize     *authorize         `json:"authorize,omitempty"`
		ActiveStreams []string           `json:"active_streams,omitempty"`
		Streams       map[string]*Stream `json:"streams,omitempty"`
	}
)

// NewMist creates new MistAPI object
func NewMist(host, login, password, livepeerToken string) *API {
	url := fmt.Sprintf("http://%s:4242/api2", host)
	urlv := fmt.Sprintf("http://%s:4242/api", host)
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

	mh := md5.New()
	h1 := md5.Sum([]byte(mapi.password))
	mh.Write([]byte(hex.EncodeToString(h1[:])))
	mh.Write([]byte(auth.Authorize.Challenge))

	hashRes := hex.EncodeToString(mh.Sum(nil))
	glog.Infof("Challenge response: %s", hashRes)
	mapi.challenge = auth.Authorize.Challenge
	mapi.challengeRepsonse = hashRes
	// mac := hmac.New(md5.New, []byte(secret))
	// mac.Write([]byte(message))
	// expectedMAC := mac.Sum(nil)
	// return hex.EncodeToString(expectedMAC)

	u := mapi.apiURL + "?minimal=0&command=" + url.QueryEscape(fmt.Sprintf(`{"authorize":{"username":"%s","password":"%s"}}`, mapi.login, hashRes))
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

// CreateStream creates new stream in Mist server
func (mapi *API) CreateStream(name, profile, segmentSize string) error {
	glog.Infof("Creating Mist stream '%s' with profile '%s'", name, profile)
	reqs := &addStreamReq{
		Minimal:   1,
		Addstream: make(map[string]*Stream),
	}
	reqs.Addstream[name] = &Stream{
		Name:        name,
		Source:      "push://",
		Segmentsize: segmentSize,
		Processes:   []*Process{{Process: "Livepeer", AccessToken: mapi.livepeerToken, TargetProfile: profile, Leastlive: "1"}},
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

	/*
		auth := &authResp{}
		err = json.Unmarshal(b, auth)
		if err != nil {
			return b
		}
		glog.Infof("challenge: %s, status: %s", auth.Authorize.Challenge, auth.Authorize.Status)
		if auth.Authorize.Status == "CHALL" {
			// need to re-authenticate
		}
	*/
	return b, nil
}

func (st *Stream) String() string {
	r := fmt.Sprintf("Name: %s\nOnline: %d\nSource: %s\n", st.Name, st.Online, st.Source)
	if st.Segmentsize != "" {
		r += fmt.Sprintf("Segment size: %sms\n", st.Segmentsize)
	}
	for _, pro := range st.Processes {
		r += fmt.Sprintf("  Process %s type %s profile %s token %s leastlive %s\n",
			pro.HumanName, pro.Process, pro.TargetProfile, pro.AccessToken, pro.Leastlive)
	}
	return r
}
