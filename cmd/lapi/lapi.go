package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
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
	rootFlagSet := flag.NewFlagSet("lapi", flag.ExitOnError)
	token := rootFlagSet.String("token", "", "Livepeer API's access token")
	presets := rootFlagSet.String("presets", "P240p30fps16x9", "Transcoding profiles")

	create := &ffcli.Command{
		Name:       "create",
		ShortUsage: "lapi create stream_name [<arg> ...]",
		ShortHelp:  "Create new stream using Livepeer API.",
		Exec: func(_ context.Context, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("Stream name should be provided")
			}
			if *token == "" {
				return fmt.Errorf("Token should be provided")
			}
			_, _, err := createStream(*token, *presets, args[0])
			return err
		},
	}

	transcode := &ffcli.Command{
		Name:       "transcode",
		ShortUsage: "lapi transcode segment_file_name [<arg> ...]",
		ShortHelp:  "Transcodes segment using Livepeer API.",
		Exec: func(_ context.Context, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("File name should be provided")
			}
			if *token == "" {
				return fmt.Errorf("Token should be provided")
			}
			return transcodeSegment(*token, *presets, args[0])
		},
	}

	root := &ffcli.Command{
		ShortUsage:  "lapi [flags] <subcommand>",
		FlagSet:     rootFlagSet,
		Subcommands: []*ffcli.Command{create, transcode},
	}

	// if err := root.ParseAndRun(context.Background(), os.Args[1:]); err != nil {
	// 	log.Fatal(err)
	// }
	if err := root.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	hostName, _ := os.Hostname()
	fmt.Println("lapi version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	// flag.Parse()

	if err := root.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func createStream(token, presets, name string) (string, *livepeer.API, error) {
	fmt.Printf("Creating new stream with name %s profiles %s\n", name, presets)
	profiles := strings.Split(presets, ",")

	lapi := livepeer.NewLivepeer(token, livepeer.ACServer, nil) // hardcode AC server for now
	lapi.Init()
	sid, err := lapi.CreateStream(name, profiles...)
	if err != nil {
		fmt.Println("Error creating stream:")
		return "", nil, err
	}
	fmt.Printf("Stream created: %+v", sid)
	return sid, lapi, nil
}

func transcodeSegment(token, presets, name string) error {
	profiles := strings.Split(presets, ",")
	sid, lapi, _ := createStream(token, presets, "lapi_stream")
	bs, err := lapi.Broadcasters()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got broadcasters list: %+v\n", bs)
	if len(bs) == 0 {
		return errors.New("Broadcasters list are empty")
	}
	fmt.Println(sid)
	base := filepath.Base(name)
	d, f := filepath.Split(name)
	ext := filepath.Ext(f)
	fn := strings.TrimSuffix(f, ext)
	fmt.Printf("base : %s d: %s f: %s ext: %s fn: %s\n", base, d, f, ext, fn)
	burl := bs[0]
	burl = "http://localhost:8935"
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return err
	}

	httpIngestURL := fmt.Sprintf("%s/live/%s/0.ts", burl, sid)
	var body io.Reader
	body = bytes.NewReader(data)
	req, err := uhttp.NewRequest("POST", httpIngestURL, body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Accept", "multipart/mixed")
	req.Header.Set("Content-Duration", strconv.FormatInt(2000, 10)) // 2sec
	hc := httpClient
	if strings.HasPrefix(httpIngestURL, "https:") {
		hc = http2Client
	}
	postStarted := time.Now()
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return errors.New(resp.Status + ": " + string(b))
	}
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return fmt.Errorf("Error getting mime type %v", err)
	}
	var segments [][]byte
	var urls []string
	if "multipart/mixed" == mediaType {
		mr := multipart.NewReader(resp.Body, params["boundary"])
		for {
			p, merr := mr.NextPart()
			if merr == io.EOF {
				break
			}
			if merr != nil {
				return fmt.Errorf("Could not process multipart part %v", merr)
			}
			mediaType, _, err := mime.ParseMediaType(p.Header.Get("Content-Type"))
			if err != nil {
				return fmt.Errorf("Error getting mime type %v", err)
			}
			body, merr := ioutil.ReadAll(p)
			if merr != nil {
				return fmt.Errorf("Error reading body err=%v", merr)
			}
			if mediaType == "application/vnd+livepeer.uri" {
				urls = append(urls, string(body))
			} else {
				fmt.Printf("Read back segment for profile=%d len=%d bytes", len(segments), len(body))
				segments = append(segments, body)
			}
		}
	}
	took := time.Since(postStarted)
	fmt.Printf("Transcoding took=%s profiles=%d", took, len(segments))
	resp.Body.Close()
	if len(segments) > 0 {
		for i, tseg := range segments {
			ofn := fmt.Sprintf("%s_%s.ts", fn, profiles[i])
			err = ioutil.WriteFile(ofn, tseg, 0644)
			if err != nil {
				return err
			}
		}
	} else if len(urls) > 0 {
		glog.Infof("Got %d urls as result: %+v", len(urls), urls)
	}
	return nil
}
