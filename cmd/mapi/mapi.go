package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2/ffcli"
	"golang.org/x/net/http2"
)

const httpTimeout = 16 * time.Second

var httpClient = &http.Client{
	Timeout: httpTimeout,
}

var http2Client = &http.Client{
	Transport: &http2.Transport{},
	Timeout:   httpTimeout,
}

func main() {
	flag.Set("logtostderr", "true")
	rootFlagSet := flag.NewFlagSet("mapi", flag.ExitOnError)
	mistCreds := rootFlagSet.String("mist-creds", "", "login:password of the Mist server")
	token := rootFlagSet.String("token", "", "Livepeer API's access token")
	host := rootFlagSet.String("host", "", "Mist server's host name")
	// presets := rootFlagSet.String("presets", "P240p30fps16x9", "Transcoding profiles")

	create := &ffcli.Command{
		Name:       "create",
		ShortUsage: "mapi create stream_name [<arg> ...]",
		ShortHelp:  "Create new stream using on the Mist server.",
		Exec: func(_ context.Context, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("Stream name should be provided")
			}
			if *token == "" {
				return fmt.Errorf("Token should be provided")
			}
			if *host == "" {
				return fmt.Errorf("Mist's host name should be provided")
			}
			if *mistCreds == "" {
				return fmt.Errorf("Mist's credentials should be provided")
			}
			createStream(*host, *mistCreds, *token, args[0])
			// _, _, err := createStream(*token, *presets, args[0])
			// return err
			return nil
		},
	}

	ls := &ffcli.Command{
		Name:       "ls",
		ShortUsage: "mapi ls ",
		ShortHelp:  "Lists streams on the Mist server.",
		Exec: func(_ context.Context, args []string) error {
			if *host == "" {
				return fmt.Errorf("Mist's host name should be provided")
			}
			if *mistCreds == "" {
				return fmt.Errorf("Mist's credentials should be provided")
			}
			return ls(*host, *mistCreds)
			// return transcodeSegment(*token, *presets, args[0])
		},
	}

	pullPicarto := &ffcli.Command{
		Name:       "picarto",
		ShortUsage: "mapi picarto ",
		ShortHelp:  "Lists streams on the Mist server.",
		Exec: func(_ context.Context, args []string) error {
			if *host == "" {
				return fmt.Errorf("Mist's host name should be provided")
			}
			if *mistCreds == "" {
				return fmt.Errorf("Mist's credentials should be provided")
			}
			if len(args) == 0 {
				return fmt.Errorf("Stream name should be provided")
			}
			return picarto(*host, args[0])
			// return transcodeSegment(*token, *presets, args[0])
		},
	}

	root := &ffcli.Command{
		ShortUsage:  "mapi [flags] <subcommand>",
		FlagSet:     rootFlagSet,
		Subcommands: []*ffcli.Command{create, ls, pullPicarto},
	}

	// if err := root.ParseAndRun(context.Background(), os.Args[1:]); err != nil {
	// 	log.Fatal(err)
	// }
	if err := root.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	// flag.Parse()
	hostName, _ := os.Hostname()
	fmt.Println("mapi version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())

	if err := root.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func createStream(host, creds, token, name string) error {
	fmt.Printf("Creating new stream with name %s\n", name)

	credsp := strings.Split(creds, ":")

	mapi := mist.NewMist(host, credsp[0], credsp[1], token)
	mapi.Login()
	mapi.CreateStream(name, "P240p30fps16x9")

	return nil
}

func ls(host, creds string) error {
	fmt.Printf("Getting list of streams from %s\n", host)
	credsp := strings.Split(creds, ":")

	mapi := mist.NewMist(host, credsp[0], credsp[1], "")
	mapi.Login()
	streams, activeStreams, err := mapi.Streams()
	if err != nil {
		panic(err)
	}
	for sn, st := range streams {
		fmt.Printf("Stream %s:\n", sn)
		fmt.Printf("%s\n", st)
	}
	if len(activeStreams) > 0 {
		fmt.Printf("Active streams: %+v", activeStreams)
	}

	return nil
}

func picarto(host, name string) error {

	u := fmt.Sprintf("http://%s:8080/hls/golive+%s/index.m3u8", host, name)
	fmt.Println(u)
	resp, err := httpClient.Do(uhttp.GetRequest(u))
	if err != nil {
		glog.Fatalf("Error authenticating to Mist server (%s) error: %v", u, err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Fatalf("===== status error contacting Mist server (%s) status %d body: %s", u, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		panic(err)
	}
	glog.Info(string(b))
	return nil
}
