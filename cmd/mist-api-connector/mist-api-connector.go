package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
	mistapi "github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/app/mistapiconnector"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff"
)

func main() {
	model.AppName = "mist-api-connector"
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")
	fs := flag.NewFlagSet("testdriver", flag.ExitOnError)

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	host := fs.String("host", "localhost", "Hostname to bind to")
	port := fs.Uint("port", 7933, "Own port")
	ownURI := fs.String("own-uri", "http://localhost:7933/", "URL at wich service will be accessible by MistServer")

	balancerHost := fs.String("balancer-host", "", "Mist's Load Balancer host")
	mistHost := fs.String("mist-host", "localhost", "Hostname of the Mist server")
	mistPort := fs.Uint("mist-port", 4242, "Port of the Mist server")
	mistCreds := fs.String("mist-creds", "", "login:password of the Mist server")
	sendAudio := fs.String("send-audio", "record", "when should we send audio?  {always|never|record}")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used by the Mist server")
	apiServer := fs.String("api-server", livepeer.ACServer, "Livepeer API server to use")
	consulURI := fs.String("consul", "", "Base URL to access Consul (for example: http://localhost:8500)")
	consulPrefix := fs.String("consul-prefix", "", "Prefix to be prepended to all created consul routes e.g. 'nyc-'")
	playbackDomain := fs.String("playback-domain", "", "regex of domain to create consul routes for (ex: playback.livepeer.live)")
	mistURL := fs.String("consul-mist-url", "", "external URL of this Mist instance (to be put in Consul) (ex: https://mist-server-0.livepeer.live)")
	baseStreamName := fs.String("base-stream-name", "", "Base stream name to be used in wildcard-based routing scheme")
	fEtcdEndpoints := fs.String("etcd-endpoints", "", "Comma-separated list of ETCD endpoints")
	etcdCaCert := fs.String("etcd-cacert", "", "ETCD CA file name")
	etcdCert := fs.String("etcd-cert", "", "ETCD client certificate file name")
	etcdKey := fs.String("etcd-key", "", "ETCD client certificate key file name")
	amqpUrl := fs.String("amqp-url", "", "RabbitMQ url")
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
	fmt.Printf("AMQP URL=%s \n", *amqpUrl)

	var mapi *mistapi.API
	mcreds := strings.Split(*mistCreds, ":")
	if len(mcreds) != 2 {
		glog.Fatal("Mist server's credentials should be in form 'login:password'")
	}
	lapi := livepeer.NewLivepeer(*apiToken, *apiServer, nil)
	lapi.Init()

	mapi = mistapi.NewMist(*mistHost, mcreds[0], mcreds[1], *apiToken, *mistPort)
	mapi.Login()
	metrics.InitCensus(hostName, model.Version, "mistconnector")
	var etcdEndpoints []string
	if len(*fEtcdEndpoints) > 0 {
		etcdEndpoints = strings.Split(*fEtcdEndpoints, ",")
	}

	var consulURL *url.URL
	var err error
	if *consulURI != "" {
		consulURL, err = url.Parse(*consulURI)
		if err != nil {
			glog.Fatalf("Error parsing Consul URL: %v", err)
		}
	}
	mc, err := mistapiconnector.NewMac(*mistHost, mapi, lapi, *balancerHost, false, consulURL, *consulPrefix,
		*playbackDomain, *mistURL, *sendAudio, *baseStreamName, etcdEndpoints, *etcdCaCert, *etcdCert, *etcdKey,
		*amqpUrl)
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
