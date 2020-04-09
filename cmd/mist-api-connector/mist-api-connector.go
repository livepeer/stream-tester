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
	mistHot string
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
	if trigger != "RTMP_PUSH_REWRITE" {
		glog.Errorf("Got unsupported trigger: '%s'", trigger)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	lines := strings.Split(bs, "\n")
	if len(lines) < 2 {
		glog.Errorf("Expected 2 lines, got %d, request \n%s", len(lines), bs)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	glog.V(model.VVERBOSE).Infof("Parsed request (%d):\n%+v", len(lines), lines)
	pu, err := url.Parse(lines[0])
	if err != nil {
		glog.Errorf("Error parsing url=%s err=%v", lines[0], err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	pp := strings.Split(pu.Path, "/")
	if len(pp) != 3 {
		glog.Errorf("URL wrongly formatted - should be in format rtmp://host/live/streamname")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	streamName := pp[2]
	glog.V(model.SHORT).Infof("Requested stream name is '%s'", streamName)
	// ask API
	stream, err := mc.lapi.GetStream(streamName)
	if err != nil || stream == nil {
		glog.Errorf("Error getting stream info from Livepeer API err=%v", err)
		if err == livepeer.ErrNotExists {
			mc.mapi.DeleteStreams(streamName)
			w.Write([]byte(lines[0]))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
	glog.V(model.DEBUG).Infof("For stream %s got info %+v", streamName, stream)

	streamName = strings.ReplaceAll(streamName, "-", "")
	preset := "P144p30fps16x9"
	if len(stream.Presets) > 0 {
		preset = stream.Presets[0]
	}
	err = mc.mapi.CreateStream(streamName, preset, "1", mc.lapi.GetServer()+"/api")
	if err != nil {
		glog.Errorf("Error creating stream on the Mist server: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write([]byte(lines[0]))
	glog.Infof("Responded with '%s'", lines[0])
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

func (mc *mac) addTrigger(triggers mistapi.TriggersMap, name, ownURI, def string) bool {
	nt := mistapi.Trigger{
		Default: def,
		Handler: ownURI,
		Sync:    true,
	}
	pr := triggers[name]
	found := false
	for _, trig := range pr {
		if trig.Default == nt.Default && trig.Handler == nt.Handler && trig.Sync == nt.Sync {
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
	added := mc.addTrigger(triggers, "RTMP_PUSH_REWRITE", ownURI, "000reallylongnonexistenstreamnamethatreallyshouldntexist000")
	added = mc.addTrigger(triggers, "DEFAULT_STREAM", ownURI, "false") || added
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
	}
}

func main() {
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
