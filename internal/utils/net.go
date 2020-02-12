package utils

import (
	"crypto/tls"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
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
	req, _ := http.NewRequest("GET", "https://api.ipify.org?format=text", nil)
	resp, err := httpDo(req, iip)
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
