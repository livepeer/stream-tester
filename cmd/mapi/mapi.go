package main

import (
	"context"
	"errors"
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
	"github.com/livepeer/stream-tester/apis/picarto"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2/ffcli"
	"golang.org/x/net/http2"
)

const httpTimeout = 16 * time.Second
const mistPort = 4242

var httpClient = &http.Client{
	Timeout: httpTimeout,
}

var http2Client = &http.Client{
	Transport: &http2.Transport{},
	Timeout:   httpTimeout,
}

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	rootFlagSet := flag.NewFlagSet("mapi", flag.ExitOnError)
	verbosity := rootFlagSet.String("v", "", "Log verbosity.  {4|5|6}")

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
		},
	}

	rm := &ffcli.Command{
		Name:       "rm",
		ShortUsage: "mapi rm [stream_name]",
		ShortHelp:  "Remove streams from the Mist server.",
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
			return rm(*host, *mistCreds, args[0])
		},
	}

	pullPicarto := &ffcli.Command{
		Name:       "pull",
		ShortUsage: "mapi picarto pull <username>",
		ShortHelp:  "Pull Picarto stream on the Mist server.",
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
			return picartoPull(*host, args[0])
			// return transcodeSegment(*token, *presets, args[0])
		},
	}

	picartoFagSet := flag.NewFlagSet("picarto-ls", flag.ExitOnError)
	country := picartoFagSet.String("country", "", "Country of the Picarto's user")
	adult := picartoFagSet.Bool("adult", false, "Adult flag")
	gaming := picartoFagSet.Bool("gaming", false, "Gaming flag")
	lsPicarto := &ffcli.Command{
		Name:       "ls",
		ShortUsage: "mapi picarto ls [-country -adult -gaming]",
		ShortHelp:  "Lists streams on the Picarto server.",
		Exec: func(_ context.Context, args []string) error {
			return picartoLS(*country, *adult, *gaming)
		},
		FlagSet: picartoFagSet,
	}

	picarto := &ffcli.Command{
		Name:        "picarto",
		ShortUsage:  "mapi picarto <subcommand> ",
		Subcommands: []*ffcli.Command{pullPicarto, lsPicarto},
	}

	root := &ffcli.Command{
		ShortUsage:  "mapi [flags] <subcommand>",
		FlagSet:     rootFlagSet,
		Subcommands: []*ffcli.Command{create, ls, rm, picarto},
	}

	// if err := root.ParseAndRun(context.Background(), os.Args[1:]); err != nil {
	// 	log.Fatal(err)
	// }
	if err := root.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)
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

	mapi := mist.NewMist(host, credsp[0], credsp[1], token, mistPort)
	mapi.Login()
	mapi.CreateStream(name, []string{"P240p30fps16x9"}, nil, "", "")

	return nil
}

func ls(host, creds string) error {
	fmt.Printf("Getting list of streams from %s\n", host)
	credsp := strings.Split(creds, ":")

	mapi := mist.NewMist(host, credsp[0], credsp[1], "", mistPort)
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

func rm(host, creds, streamName string) error {
	if len(streamName) < 2 {
		return errors.New("Stream name too short")
	}
	credsp := strings.Split(creds, ":")

	mapi := mist.NewMist(host, credsp[0], credsp[1], "", mistPort)
	mapi.Login()
	streams, _, err := mapi.Streams()
	if err != nil {
		panic(err)
	}
	for sn := range streams {
		if strings.HasPrefix(sn, streamName) {
			mapi.DeleteStreams(sn)
		}
	}
	return nil
}

func picartoLS(country string, adult, gaming bool) error {
	users, err := picarto.GetOnlineUsers(country, adult, gaming)
	if err != nil {
		return err
	}
	fmt.Printf("Got %d online users:\n", len(users))
	for _, user := range users {
		fmt.Printf("User %s viewers %d adult %v gaming %v\n", user.Name, user.Viewers, user.Adult, user.Gaming)
	}
	return nil
}

func picartoPull(host, name string) error {

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
