package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
	mistapi "github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/app/mistapiconnector"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff"
)

func main() {
	model.AppName = "mist-api-connector"
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")
	fs := flag.NewFlagSet("testdriver", flag.ExitOnError)

	mistJson := fs.Bool("j", false, "Print application info as json")

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	host := fs.String("host", "localhost", "Hostname to bind to")
	port := fs.Uint("port", 7933, "Own port")
	ownURI := fs.String("own-uri", "http://localhost:7933/", "URL at wich service will be accessible by MistServer")

	balancerHost := fs.String("balancer-host", "", "Mist's Load Balancer host")
	mistHost := fs.String("mist-host", "localhost", "Hostname of the Mist server")
	mistPort := fs.Uint("mist-port", 4242, "Port of the Mist server")
	mistCreds := fs.String("mist-creds", "", "login:password of the Mist server")
	mistConnectTimeout := fs.Duration("mist-connect-timeout", 5*time.Minute, "Max time to wait attempting to connect to Mist server")
	sendAudio := fs.String("send-audio", "record", "when should we send audio?  {always|never|record}")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used by the Mist server")
	apiServer := fs.String("api-server", livepeer.ACServer, "Livepeer API server to use")
	routePrefix := fs.String("route-prefix", "", "Prefix to be prepended to all created routes e.g. 'nyc-'")
	playbackDomain := fs.String("playback-domain", "", "regex of domain to create routes for (ex: playback.livepeer.live)")
	mistURL := fs.String("route-mist-url", "", "external URL of this Mist instance (used for routing) (ex: https://mist-server-0.livepeer.live)")
	baseStreamName := fs.String("base-stream-name", "", "Base stream name to be used in wildcard-based routing scheme")
	fEtcdEndpoints := fs.String("etcd-endpoints", "", "Comma-separated list of ETCD endpoints")
	etcdCaCert := fs.String("etcd-cacert", "", "ETCD CA file name")
	etcdCert := fs.String("etcd-cert", "", "ETCD client certificate file name")
	etcdKey := fs.String("etcd-key", "", "ETCD client certificate key file name")
	amqpUrl := fs.String("amqp-url", "", "RabbitMQ url")
	ownRegion := fs.String("own-region", "", "Identifier of the region where the service is running, used for mapping external data back to current region")
	_ = fs.String("config", "", "config file (optional)")

	consulPrefix := fs.String("consul-prefix", "", "DEPRECATED - use --route-prefix")
	consulMistURL := fs.String("consul-mist-url", "", "DEPRECATED - use --route-mist-url")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("MAPIC"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)

	if *mistJson {
		mistconnector.PrintMistConfigJson("mist-api-connector", "", "Mist API Connector", model.Version, fs)
		return
	}

	hostName, _ := os.Hostname()
	fmt.Println("mist-api-connector version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())

	if *routePrefix == "" && *consulPrefix != "" {
		glog.Warningln("--consul-prefix is deprecated, use --route-prefix instead")
		routePrefix = consulPrefix
	}
	if *mistURL == "" && *consulMistURL != "" {
		glog.Warningln("--consul-mist-url is deprecated, use --route-mist-url instead")
		mistURL = consulMistURL
	}

	var mapi *mistapi.API
	mcreds := strings.Split(*mistCreds, ":")
	if len(mcreds) != 2 {
		glog.Fatal("Mist server's credentials should be in form 'login:password'")
	}
	lapi := livepeer.NewLivepeer(*apiToken, *apiServer, nil)
	lapi.Init()

	mapi = mistapi.NewMist(*mistHost, mcreds[0], mcreds[1], *apiToken, *mistPort)
	ensureLoggedIn(mapi, *mistConnectTimeout)
	metrics.InitCensus(hostName, model.Version, "mistconnector")
	var etcdEndpoints []string
	if len(*fEtcdEndpoints) > 0 {
		etcdEndpoints = strings.Split(*fEtcdEndpoints, ",")
	}

	opts := mistapiconnector.MacOptions{
		NodeID:         hostName,
		MistHost:       *mistHost,
		MistAPI:        mapi,
		LivepeerAPI:    lapi,
		BalancerHost:   *balancerHost,
		RoutePrefix:    *routePrefix,
		PlaybackDomain: *playbackDomain,
		MistURL:        *mistURL,
		BaseStreamName: *baseStreamName,
		CheckBandwidth: false,
		SendAudio:      *sendAudio,
		EtcdEndpoints:  etcdEndpoints,
		EtcdCaCert:     *etcdCaCert,
		EtcdCert:       *etcdCert,
		EtcdKey:        *etcdKey,
		AMQPUrl:        *amqpUrl,
		OwnRegion:      *ownRegion,
	}
	mc, err := mistapiconnector.NewMac(opts)
	if err != nil {
		glog.Fatalf("Error creating mist-api-connector %v", err)
	}
	if err := mc.SetupTriggers(*ownURI); err != nil {
		glog.Fatal(err)
	}
	err = mc.StartServer(fmt.Sprintf("%s:%d", *host, *port))
	glog.Infof("Start shutting down host=%s err=%v", hostName, err)
	err = <-mc.SrvShutCh()
	glog.Infof("Done shutting down host=%s err=%v", hostName, err)
}

func ensureLoggedIn(mapi *mistapi.API, timeout time.Duration) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		err := mapi.Login()
		if err == nil {
			return
		}

		var netErr net.Error
		if !errors.As(err, &netErr) {
			glog.Fatalf("Fatal non-network error logging to mist. err=%q", err)
		}
		select {
		case <-deadline.C:
			glog.Fatalf("Failed to login to mist after %s. err=%q", timeout, netErr)
		case <-time.After(1 * time.Second):
			glog.Errorf("Retrying after network error logging to mist. err=%q", netErr)
		}
	}
}
