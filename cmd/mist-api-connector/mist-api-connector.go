package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/apis/mist"
	mistapi "github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff"
)

type mac struct {
	mapi    *mist.API
	lapi    *livepeer.API
	pub2id  map[string]string // public key to stream id
	mistHot string
}

// LivepeerProfiles2MistProfiles converts Livepeer's API profiles to Mist's ones
func LivepeerProfiles2MistProfiles(lps []livepeer.Profile) []mist.Profile {
	var res []mist.Profile
	for _, p := range lps {
		mp := mist.Profile{
			Name:      p.Name,
			HumanName: p.Name,
			Width:     p.Width,
			Height:    p.Height,
			Fps:       p.Fps,
			Bitrate:   p.Bitrate,
		}
		res = append(res, mp)
	}
	return res
}

func (mc *mac) handleDefaultStreamTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	bs := string(b)
	trigger := r.Header.Get("X-Trigger")
	glog.V(model.VERBOSE).Infof("Got request (%s):\n%s", trigger, bs)
	glog.V(model.VERBOSE).Infof("User agent: %s", r.UserAgent())
	glog.V(model.VERBOSE).Infof("Mist version: %s", r.Header.Get("X-Version"))
	if trigger == "DEFAULT_STREAM" {
		// We should get this in two cases:
		// 1. When in RTMP_PUSH_REWRITE we got request for unknown stream and thus
		//    haven't created new stream in Mist
		// 2. When someone pulls HLS for stream that exists but is not active (no
		//    RTMP stream coming in).
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if trigger == "LIVE_BANDWIDTH" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("yes"))
		return
	}
	lines := strings.Split(bs, "\n")
	if trigger == "CONN_CLOSE" {
		if len(lines) < 3 {
			glog.Errorf("Expected 3 lines, got %d, request \n%s", len(lines), bs)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if lines[2] == "RTMP" {
			playbackID := lines[0]
			if id, has := mc.pub2id[playbackID]; has {
				err := mc.lapi.SetActive(id, false)
				if err != nil {
					glog.Error(err)
				}
				delete(mc.pub2id, playbackID)
			}
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if trigger != "RTMP_PUSH_REWRITE" {
		glog.Errorf("Got unsupported trigger: '%s'", trigger)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(lines) < 2 {
		glog.Errorf("Expected 2 lines, got %d, request \n%s", len(lines), bs)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	glog.V(model.VVERBOSE).Infof("Parsed request (%d):\n%+v", len(lines), lines)
	pu, err := url.Parse(lines[0])
	responseURL := lines[0]
	if err != nil {
		glog.Errorf("Error parsing url=%s err=%v", lines[0], err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	pp := strings.Split(pu.Path, "/")
	if len(pp) != 3 {
		glog.Errorf("URL wrongly formatted - should be in format rtmp://mist.host/live/streamKey")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	streamKey := pp[2]
	glog.V(model.SHORT).Infof("Requested stream key is '%s'", streamKey)
	// ask API
	stream, err := mc.lapi.GetStreamByKey(streamKey)
	if err != nil || stream == nil {
		glog.Errorf("Error getting stream info from Livepeer API err=%v", err)
		/*
			if err == livepeer.ErrNotExists {
				// mc.mapi.DeleteStreams(streamKey)
				w.Write([]byte(lines[0]))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		*/
		w.WriteHeader(http.StatusNotFound)
		return
	}
	glog.V(model.DEBUG).Infof("For stream %s got info %+v", streamKey, stream)

	if stream.PlaybackID != "" {
		mc.pub2id[stream.PlaybackID] = stream.ID
		streamKey = stream.PlaybackID
		streamKey = strings.ReplaceAll(streamKey, "-", "")
		pp[2] = streamKey
		pu.Path = strings.Join(pp, "/")
		responseURL = pu.String()
		err := mc.lapi.SetActive(stream.ID, true)
		if err != nil {
			glog.Error(err)
		}
	} else {
		streamKey = strings.ReplaceAll(streamKey, "-", "")
	}
	if stream.Deleted {
		mc.mapi.DeleteStreams(streamKey)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if len(stream.Presets) == 0 && len(stream.Profiles) == 0 {
		stream.Presets = append(stream.Presets, "P144p30fps16x9")
	}
	// err = mc.mapi.CreateStream(streamKey, stream.Presets, LivepeerProfiles2MistProfiles(stream.Profiles), "1", mc.lapi.GetServer()+"/api/stream/"+stream.ID)
	err = mc.mapi.CreateStream(streamKey, stream.Presets, LivepeerProfiles2MistProfiles(stream.Profiles), "1", "http://host.docker.internal:3004/api/stream/"+stream.ID)
	if err != nil {
		glog.Errorf("Error creating stream on the Mist server: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write([]byte(responseURL))
	glog.Infof("Responded with '%s'", responseURL)
}

func (mc *mac) webServerHandlers() *http.ServeMux {
	mux := http.NewServeMux()
	utils.AddPProfHandlers(mux)
	mux.Handle("/metrics", utils.InitPrometheusExporter("mistconnector"))

	mux.HandleFunc("/", mc.handleDefaultStreamTrigger)
	return mux
}

func (mc *mac) startServer(bindAddr string) {
	mux := mc.webServerHandlers()
	srv := &http.Server{
		Addr:    bindAddr,
		Handler: mux,
	}

	glog.Info("Web server listening on ", bindAddr)
	srv.ListenAndServe()
}

func (mc *mac) addTrigger(triggers mistapi.TriggersMap, name, ownURI, def, params string, sync bool) bool {
	nt := mistapi.Trigger{
		Default: def,
		Handler: ownURI,
		Sync:    sync,
		Params:  params,
	}
	pr := triggers[name]
	found := false
	for _, trig := range pr {
		if trig.Default == nt.Default && trig.Handler == nt.Handler && trig.Sync == nt.Sync && trig.Params == params {
			found = true
			break
		}
	}
	if !found {
		pr = append(pr, nt)
		triggers[name] = pr
	}
	return !found
}

func (mc *mac) setupTriggers(ownURI string) error {
	triggers, err := mc.mapi.GetTriggers()
	if err != nil {
		glog.Error(err)
		return err
	}
	if triggers == nil {
		triggers = make(mistapi.TriggersMap)
	}
	added := mc.addTrigger(triggers, "RTMP_PUSH_REWRITE", ownURI, "000reallylongnonexistenstreamnamethatreallyshouldntexist000", "", true)
	added = mc.addTrigger(triggers, "DEFAULT_STREAM", ownURI, "false", "", true) || added
	added = mc.addTrigger(triggers, "LIVE_BANDWIDTH", ownURI, "false", "100000", true) || added
	added = mc.addTrigger(triggers, "CONN_CLOSE", ownURI, "", "", false) || added
	if added {
		err = mc.mapi.SetTriggers(triggers)
	}
	return err
}

func newMac(mistHost string, mapi *mist.API, lapi *livepeer.API) *mac {
	return &mac{
		mistHot: mistHost,
		mapi:    mapi,
		lapi:    lapi,
		pub2id:  make(map[string]string), // public key to stream id
	}
}

func main() {
	model.AppName = "mist-api-connector"
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")
	fs := flag.NewFlagSet("testdriver", flag.ExitOnError)

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	host := fs.String("host", "localhost", "Hostname to bind to")
	port := fs.Uint("port", 7933, "Own port")
	ownURI := fs.String("own-uri", "http://localhost:7933/", "URL at wich service will be accessible by MistServer")

	mistHost := fs.String("mist-host", "localhost", "Hostname of the Mist server")
	mistCreds := fs.String("mist-creds", "", "login:password of the Mist server")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used by the Mist server")
	apiServer := fs.String("api-server", livepeer.ACServer, "Livepeer API server to use")
	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("MAPIC"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)
	hostName, _ := os.Hostname()
	fmt.Println("mist-api-connector version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())

	var mapi *mistapi.API
	mcreds := strings.Split(*mistCreds, ":")
	if len(mcreds) != 2 {
		glog.Fatal("Mist server's credentials should be in form 'login:password'")
	}
	lapi := livepeer.NewLivepeer(*apiToken, *apiServer, nil)
	lapi.Init()

	mapi = mistapi.NewMist(*mistHost, mcreds[0], mcreds[1], *apiToken)
	mapi.Login()

	mc := newMac(*mistHost, mapi, lapi)
	if err := mc.setupTriggers(*ownURI); err != nil {
		glog.Fatal(err)
	}
	mc.startServer(fmt.Sprintf("%s:%d", *host, *port))
}
