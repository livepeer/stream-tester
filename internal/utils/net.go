package utils

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"
	"time"

	"contrib.go.opencensus.io/exporter/prometheus"
	"github.com/golang/glog"
	rprom "github.com/prometheus/client_golang/prometheus"
)

var (
	httpClients = make(map[string]*http.Client)
)

func httpDo(req *http.Request, sourceIP string) (*http.Response, error) {
	if sourceIP == "" {
		return http.DefaultClient.Do(req)
	}
	client, has := httpClients[sourceIP]
	if !has {
		localAddress := &net.TCPAddr{
			IP: net.ParseIP(sourceIP), // a secondary local IP I assigned to me
		}
		if localAddress.IP == nil {
			panic("Bad ip address: " + sourceIP)
		}
		// log.Printf("Making http request using ip addr '%s'", localAddress)
		transport := &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: false},
			Dial: (&net.Dialer{
				Timeout:   8 * time.Second,
				KeepAlive: 8 * time.Second,
				LocalAddr: localAddress}).Dial, TLSHandshakeTimeout: 8 * time.Second}

		client = &http.Client{
			Transport: transport,
		}
		httpClients[sourceIP] = client
	}
	return client.Do(req)
}

func getExternalIP(iip string) string {
	// TODO probably should put this (along w wizard GETs) into common code
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://api.ipify.org?format=text", nil)
	resp, err := httpDo(req, iip)
	cancel()
	if err != nil {
		glog.Error("Could not look up public IP address")
		return ""
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Error("Could not look up public IP address")
		return ""
	}
	return strings.TrimSpace(string(body))
}

// GetIPs returns list of inter IPs
func GetIPs() [][]string {
	ips := make([][]string, 0, 1)
	ifaces, err := net.Interfaces()
	if err != nil {
		glog.Fatal(err)
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		// handle err
		if err != nil {
			panic(err)
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			// log.Printf("==+++ addres %s of len %d", ip, len(ip))
			if ip.To4() == nil {
				continue
			}
			extIP := getExternalIP(ip.String())
			if extIP == "" {
				// can't connect to Internet through this interface
				// continue
			}
			ips = append(ips, []string{ip.String(), extIP})
		}
	}
	return ips
}

// WaitForTCP trries to establish TCP connectin for a specified time
func WaitForTCP(waitForTarget time.Duration, uri string) error {
	var u *url.URL
	var err error
	if u, err = url.Parse(uri); err != nil {
		return err
	}
	if u.Port() == "" {
		switch u.Scheme {
		case "rtmp":
			u.Host = u.Host + ":1935"
		}
	}
	dailer := net.Dialer{Timeout: 2 * time.Second}
	started := time.Now()
	var conn net.Conn
	for {
		if conn, err = dailer.Dial("tcp", u.Host); err != nil {
			time.Sleep(4 * time.Second)
			if time.Since(started) > waitForTarget {
				return fmt.Errorf("Can't connect to '%s' for more than %s", uri, waitForTarget)
			}
			continue
		}
		conn.Close()
		break
	}
	return nil
}

// InitPrometheusExporter init prometheus exporter
func InitPrometheusExporter(namespace string) *prometheus.Exporter {

	registry := rprom.NewRegistry()
	registry.MustRegister(rprom.NewProcessCollector(rprom.ProcessCollectorOpts{}))
	registry.MustRegister(rprom.NewGoCollector())

	pe, err := prometheus.NewExporter(prometheus.Options{
		Namespace: namespace,
		Registry:  registry,
	})
	if err != nil {
		glog.Fatalf("Failed to create the Prometheus stats exporter: %v", err)
	}

	return pe
}

// AddPProfHandlers add standard handlers from pprof package
func AddPProfHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
