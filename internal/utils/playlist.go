package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/model"
)

var (
	// ErrStreamOpenFailed ...
	ErrStreamOpenFailed = errors.New("Stream open failed")

	mhttpClient = &http.Client{
		// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
		// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		// Transport: &http2.Transport{AllowHTTP: true},
		Timeout: 4 * time.Second,
	}
)

func timedout(e error) bool {
	t, ok := e.(interface {
		Timeout() bool
	})
	return ok && t.Timeout() || (e != nil && strings.Contains(e.Error(), "Client.Timeout"))
}

// DownloadMasterPlaylist downloads master playlist
func DownloadMasterPlaylist(uri string) (*m3u8.MasterPlaylist, error) {
	resp, err := mhttpClient.Do(uhttp.GetRequest(uri))
	if err != nil {
		to := timedout(err)
		glog.Infof("===== get error (timed out: %v eof: %v) getting master playlist %s: %v", to, errors.Is(err, io.EOF), uri, err)
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		glog.Infof("===== error getting master playlist body uri=%s err=%v", uri, err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("===== status error getting master playlist %s: %v (%s) body: %s", uri, resp.StatusCode, resp.Status, string(b))
		return nil, err
	}
	if strings.Contains(string(b), "Stream open failed") {
		glog.Errorf("Master playlist stream open failed uri=%s", uri)
		return nil, ErrStreamOpenFailed
	}
	mpl := m3u8.NewMasterPlaylist()
	// err = mpl.DecodeFrom(resp.Body, true)
	err = mpl.Decode(*bytes.NewBuffer(b), true)
	// resp.Body.Close()
	if err != nil {
		glog.Infof("===== error getting master playlist uri=%s err=%v", uri, err)
		return nil, err
	}
	glog.V(model.VVERBOSE).Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), uri)
	glog.V(model.VVERBOSE).Info(mpl)
	return mpl, nil
}
