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
	"sort"
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

var server = livepeer.ACServer

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	rootFlagSet := flag.NewFlagSet("lapi", flag.ExitOnError)
	verbosity := rootFlagSet.String("v", "", "Log verbosity.  {4|5|6}")

	token := rootFlagSet.String("token", "", "Livepeer API's access token")
	presets := rootFlagSet.String("presets", "P240p30fps16x9", "Transcoding profiles")
	fServer := rootFlagSet.String("server", livepeer.ACServer, "API server to use")
	streamID := rootFlagSet.String("stream-id", "", "ID of existing stream to use for transcoding")
	seq := rootFlagSet.String("seq", "0", "Use as name when transcoding and saving results")
	outFmt := rootFlagSet.String("out-fmt", "ts", "Output format (ts or mp4)")

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
				return fmt.Errorf("file name should be provided")
			}
			if *token == "" && *streamID == "" {
				return fmt.Errorf("token or stream id should be provided")
			}
			return transcodeSegment(*token, *presets, *streamID, *seq, *outFmt, args[0])
		},
	}

	sessions := &ffcli.Command{
		Name:       "sessions",
		ShortUsage: "lapi sessions streamId ",
		ShortHelp:  "List user sessions for stream",
		Exec: func(_ context.Context, args []string) error {
			if *token == "" {
				return fmt.Errorf("token should be provided")
			}
			if len(args) == 0 && *streamID == "" {
				return fmt.Errorf("stream id should be provided")
			}
			if *streamID == "" {
				*streamID = args[0]
			}
			return listSessions(*token, *streamID)
		},
	}

	ls := &ffcli.Command{
		Name:       "ls",
		ShortUsage: "lapi ls",
		ShortHelp:  "Lists available broadcasters.",
		Exec: func(_ context.Context, args []string) error {
			if *token == "" {
				return fmt.Errorf("token should be provided")
			}
			lapi := livepeer.NewLivepeer(*token, server, nil) // hardcode AC server for now
			lapi.Init()
			bs, err := lapi.Broadcasters()
			if err != nil {
				return err
			}
			fmt.Printf("Got broadcasters:\n%s\n", strings.Join(bs, "\n"))
			return nil
		},
	}

	setActive := &ffcli.Command{
		Name:       "setactive",
		ShortUsage: "lapi setactive streamId [true|false]",
		ShortHelp:  "makes /setactive call",
		Exec: func(_ context.Context, args []string) error {
			if *token == "" {
				return fmt.Errorf("Token should be provided")
			}
			if len(args) == 0 {
				return fmt.Errorf("Stream ID should be provided")
			}
			lapi := livepeer.NewLivepeer(*token, server, nil)
			lapi.Init()
			active := false
			if len(args) > 1 && args[1] == "true" {
				active = true
			}
			ok, err := lapi.SetActive(args[0], active, time.Now())
			if err != nil {
				return err
			}
			fmt.Printf("SetActive response: %v", ok)
			return nil
		},
	}

	root := &ffcli.Command{
		ShortUsage:  "lapi [flags] <subcommand>",
		FlagSet:     rootFlagSet,
		Subcommands: []*ffcli.Command{create, transcode, ls, setActive, sessions},
	}

	// if err := root.ParseAndRun(context.Background(), os.Args[1:]); err != nil {
	// 	log.Fatal(err)
	// }
	if err := root.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)
	hostName, _ := os.Hostname()
	fmt.Println("lapi version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	// flag.Parse()
	server = *fServer

	if err := root.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func createStream(token, presets, name string) (string, *livepeer.API, error) {
	fmt.Printf("Creating new stream with name %s profiles %s\n", name, presets)
	profiles := strings.Split(presets, ",")

	lapi := livepeer.NewLivepeer(token, server, nil) // hardcode AC server for now
	lapi.Init()
	sid, err := lapi.CreateStream(name, profiles...)
	if err != nil {
		fmt.Println("Error creating stream:")
		return "", nil, err
	}
	fmt.Printf("Stream created: %+v", sid)
	return sid, lapi, nil
}

func listSessions(token, id string) error {
	// profiles := strings.Split(presets, ",")
	lapi := livepeer.NewLivepeer(token, server, nil)
	lapi.Init()
	sessions, err := lapi.GetSessionsNewR(id, false)
	if err != nil {
		panic(err)
	}
	glog.Infof("For stream id=%s got %d sessions:", id, len(sessions))
	for _, sess := range sessions {
		glog.Infof("%+v", sess)
		glog.Infof("id %s recordingStatus %s recordingUrl %s Mp4Url %s", sess.ID, sess.RecordingStatus, sess.RecordingURL, sess.Mp4Url)
	}
	return nil
}

func transcodeSegment(token, presets, sid, seq, outFmt, name string) error {
	var err error
	// profiles := strings.Split(presets, ",")
	lapi := livepeer.NewLivepeer(token, server, nil)
	lapi.Init()
	if sid == "" {
		// sid, _, err = createStream(token, presets, "lapi_stream")
		// prof := ffmpeg.P720p25fps16x9
		prof := livepeer.StandardProfiles[3]
		prof.Gop = ""
		prof.Profile = "H264High"
		prof.Profile = "h264high"
		prof.Fps = 0
		prof.FpsDen = 0
		resp, err := lapi.CreateStreamEx("lapi_stream_2", false, nil, prof)
		if err != nil {
			panic(err)
		}
		sid = resp.ID
	}
	bs, err := lapi.Broadcasters()
	if err != nil {
		panic(err)
	}
	sort.Strings(bs)
	fmt.Printf("Got broadcasters list: %+v\n", bs)
	if len(bs) == 0 {
		return errors.New("broadcasters list are empty")
	}
	// base := filepath.Base(name)
	_, f := filepath.Split(name)
	ext := filepath.Ext(f)
	fn := strings.TrimSuffix(f, ext)
	// fmt.Printf("base : %s d: %s f: %s ext: %s fn: %s\n", base, d, f, ext, fn)
	burl := bs[0]
	// burl = "http://localhost:8935"
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return err
	}

	httpIngestURL := fmt.Sprintf("%s/live/%s/%s.%s", burl, sid, seq, outFmt)
	fmt.Printf("POSTing %s\n", httpIngestURL)
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		return errors.New(resp.Status + ": " + string(b))
	}
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return fmt.Errorf("Error getting mime type %v", err)
	}
	var segments [][]byte
	var segmentsNames []string
	var urls []string
	if "multipart/mixed" == mediaType {
		mr := multipart.NewReader(resp.Body, params["boundary"])
		i := 0
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
			disposition, dispParams, err := mime.ParseMediaType(p.Header.Get("Content-Disposition"))
			if err != nil {
				return fmt.Errorf("Error getting content disposition %v", err)
			}
			fmt.Printf("Response number %d content length %s, type %s rendition name %s disposition %s file name %s\n",
				i, p.Header.Get("Content-Length"), mediaType, p.Header.Get("Rendition-Name"), disposition, dispParams["filename"])
			body, merr := ioutil.ReadAll(p)
			if merr != nil {
				return fmt.Errorf("Error reading body err=%v", merr)
			}
			if mediaType == "application/vnd+livepeer.uri" {
				urls = append(urls, string(body))
			} else {
				fmt.Printf("Read back segment for profile=%d len=%d bytes\n", len(segments), len(body))
				segments = append(segments, body)
				segmentsNames = append(segmentsNames, dispParams["filename"])
			}
			i++
		}
	}
	took := time.Since(postStarted)
	fmt.Printf("Transcoding took=%s profiles=%d", took, len(segments))
	if len(segments) > 0 {
		for i, tseg := range segments {
			// ofn := fmt.Sprintf("%s_%s.ts", fn, profiles[i])
			ofn := fmt.Sprintf("%s_%s_%s", fn, seq, segmentsNames[i])
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
