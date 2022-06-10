package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/livepeer/go-livepeer/cmd/livepeer/starter"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/joy4/format"
	streamtesterMetrics "github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/server"
	"github.com/peterbourgon/ff"
	"go.opencensus.io/stats/view"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	apiModels "github.com/livepeer/leaderboard-serverless/models"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/model"
	streamerModel "github.com/livepeer/stream-tester/model"
	promClient "github.com/prometheus/client_golang/api"
	promAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	promModels "github.com/prometheus/common/model"
)

const defaultHost = "127.0.0.1"
const streamTesterPort = "7934"
const streamTesterLapiToken = ""
const streamTesterMistCreds = ""
const prometheusPort = "9090"
const bcastMediaPort = "8935"
const bcastRTMPPort = "1935"
const bcastCliPort = "7935"

const refreshWait = 70 * time.Second
const httpTimeout = 8 * time.Second
const bcastReadyTimeout = 10 * time.Minute

const numSegments = 15

var start time.Time

func init() {
	format.RegisterAll()
}

type broadcasterConfig struct {
	network         *string
	ethUrl          *string
	datadir         *string
	ethPassword     *string
	maxTicketEV     *string
	maxPricePerUnit *int
}

func main() {
	flag.Set("logtostderr", "true")

	vFlag := flag.Lookup("v")
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	verbosity := flag.String("v", "6", "Log verbosity.  {4|5|6}")

	region := flag.String("region", "", "Region this service is operating in")
	streamTester := flag.String("streamtester", "", "Address for stream-tester server instance")
	broadcaster := flag.String("broadcaster", "", "Broadcaster address")
	metrics := flag.String("metrics", "", "Broadcaster metrics port")
	media := flag.String("media", bcastMediaPort, "Broadcaster HTTP port")
	rtmp := flag.String("rtmp", bcastRTMPPort, "broadcaster RTMP port")
	leaderboard := flag.String("leaderboard", "127.0.0.1:3001", "HTTP Address of the serverless leadearboard API")
	leaderboardSecret := flag.String("leaderboard-secret", "", "Secret for the Leaderboard API")

	subgraph := flag.String("subgraph", "https://api.thegraph.com/subgraphs/name/livepeer/livepeer-canary", "Livepeer subgraph URL")

	// Video config
	videoFile := flag.String("video", "official_test_source_2s_keys_24pfs_30s.mp4", "video file to use, has to be present in stream-tester root")
	numProfiles := flag.Int("profiles", 3, "number of video profiles to use on the broadcaster")
	presets := flag.String("presets", "P240p30fps16x9,P360p30fps16x9", "video profile presets to use for HTTP ingest")
	repeat := flag.Int("repeat", 1, "number of times to repeat the stream")
	simultaneous := flag.Int("simultaneous", 1, "number of times to run the stream simultaneously")

	// randomSample will sample one transcoded rendition and source segment randomly per stream
	randomSample := flag.Bool("randomsample", false, "randomly sample a source and transcoded segment per stream")
	gsBucket := flag.String("gsbucket", "", "Google storage bucket to store segments")
	gsKey := flag.String("gskey", "", "Google Storage private key (in json format)")

	// Embedded Broadcaster config
	var bCfg broadcasterConfig
	bCfg.network = flag.String("network", "arbitrum-one-rinkeby", "Network to connect to")
	bCfg.ethUrl = flag.String("ethUrl", "https://rinkeby.arbitrum.io/rpc", "Ethereum node JSON-RPC URL")
	bCfg.datadir = flag.String("datadir", "", "Directory that data is stored in")
	bCfg.ethPassword = flag.String("ethPassword", "", "Password for existing Eth account address")
	bCfg.maxTicketEV = flag.String("maxTicketEV", "3000000000000", "The maximum acceptable expected value for PM tickets")
	bCfg.maxPricePerUnit = flag.Int("maxPricePerUnit", 0, "The maximum transcoding price (in wei) per 'pixelsPerUnit' a broadcaster is willing to accept. If not set explicitly, broadcaster is willing to accept ANY price")

	// Config file
	_ = flag.String("config", "", "Config file in the format 'key value', flags and env vars take precedence over the config file")
	err := ff.Parse(flag.CommandLine, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithEnvVarPrefix("OT"),
		ff.WithConfigFileParser(ff.PlainParser),
	)
	vFlag.Value.Set(*verbosity)

	if *region == "" {
		log.Fatal("region is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *streamTester == "" {
		startEmbeddedStreamTester(ctx)
	}

	var bcastHost string
	if *streamTester == "" && *broadcaster == "" {
		bcastHost = defaultHost
		startEmbeddedBroadcaster(ctx, bCfg, *presets)
	} else {
		bcastHost = *broadcaster
	}

	var embeddedBcastMetrics *broadcasterMetrics
	if *streamTester == "" && *broadcaster == "" && *metrics == "" {
		glog.Infof("Using embedded broadcaster metrics")
		embeddedBcastMetrics = &broadcasterMetrics{}
		view.RegisterExporter(embeddedBcastMetrics)
	}

	metricsURL := defaultAddr(*metrics, defaultHost, prometheusPort)
	streamTesterURL := defaultAddr(*streamTester, defaultHost, streamTesterPort)
	leaderboardURL := defaultAddr(*leaderboard, defaultHost, "3001")
	subgraphURL := defaultAddr(*subgraph, defaultHost, "8080")
	broadcasterURL := defaultAddr(fmt.Sprintf("%v:%v", *broadcaster, *media), defaultHost, "8935")

	rtmpUint, err := strconv.ParseUint(*rtmp, 10, 16)
	if err != nil {
		log.Fatal(err)
	}

	mediaUint, err := strconv.ParseUint(*media, 10, 16)
	if err != nil {
		log.Fatal(err)
	}

	profiles := strings.Split(*presets, ",")

	streamer, err := newStreamerClient(streamTesterURL, metricsURL, leaderboardURL, *leaderboardSecret, subgraphURL, broadcasterURL, profiles)
	if err != nil {
		log.Fatal(err)
	}

	orchestrators, err := streamer.getOrchestrators()
	if err != nil {
		log.Fatal(err)
	}
	testers.Bucket = *gsBucket
	testers.CredsJSON = *gsKey

	var summary statsSummary
	start = time.Now()

	glog.Infof("Waiting for broadcaster to be ready")
	waitUntilBroadcasterIsReady(ctx, bcastHost)

	glog.Infof("Starting to test orchestrators")
	for _, o := range orchestrators {
		time.Sleep(refreshWait)
		if embeddedBcastMetrics != nil {
			embeddedBcastMetrics.reset()
		}

		req := &streamerModel.StartStreamsReq{
			Host:            bcastHost,
			RTMP:            uint16(rtmpUint),
			Media:           uint16(mediaUint),
			Repeat:          uint(*repeat),
			Simultaneous:    uint(*simultaneous),
			Orchestrators:   []string{o.ServiceURI},
			ProfilesNum:     *numProfiles,
			Presets:         *presets,
			DoNotClearStats: false,
			MeasureLatency:  false,
			HTTPIngest:      true,
			FileName:        *videoFile,
		}

		startTime := time.Now().Unix()
		mid, err := streamer.startStream(req)
		if err != nil {
			glog.Error(err)
			continue
		}

		var (
			ctx    context.Context
			cancel context.CancelFunc
		)
		randErr := make(chan error)
		if *randomSample {
			rand.Seed(time.Now().UnixNano())
			ctx, cancel = context.WithCancel(context.Background())
			go func(randErr chan error) {
				randErr <- streamer.randomSample(ctx, mid, o.Address)
			}(randErr)
		}

		glog.Infof("Started stream orchestrator=%v maninfestID=%v", o.Address, mid)

		apiStats := &apiModels.Stats{
			Region:       *region,
			Timestamp:    startTime,
			Orchestrator: o.Address,
		}

		// Make sure manifest ID exists before getting stats
		time.Sleep(15 * time.Second)

		// wait for stream to finish transcoding
		streamerStats, err := streamer.getFinishedStats(mid)
		if err != nil {
			glog.Error(err)
			continue
		}

		apiStats.SegmentsSent = streamerStats.SentSegments
		apiStats.SegmentsReceived = streamerStats.DownloadedSegments
		// This calculation requires HTTP ingest to be correct
		apiStats.SuccessRate = (float64(apiStats.SegmentsReceived) / float64(*numProfiles) / float64(apiStats.SegmentsSent))

		apiStats.SegDuration = avgMetric(streamer, embeddedBcastMetrics, "source_segment_duration_seconds")
		apiStats.RoundTripTime = avgMetric(streamer, embeddedBcastMetrics, "transcode_overall_latency_seconds")
		apiStats.UploadTime = avgMetric(streamer, embeddedBcastMetrics, "upload_time_seconds")
		apiStats.DownloadTime = avgMetric(streamer, embeddedBcastMetrics, "download_time_seconds")

		transcodeTime := apiStats.RoundTripTime - apiStats.UploadTime - apiStats.DownloadTime
		if transcodeTime > 0 {
			apiStats.TranscodeTime = transcodeTime
		}

		apiStats.Errors = errorCount(streamer, embeddedBcastMetrics)

		if err := streamer.postStats(apiStats); err != nil {
			glog.Error(err)
			continue
		}
		summary.add(apiStats)

		// if we haven't found a random sample by now cancel and wait for the backup attempt to complete before returning
		if *randomSample {
			cancel()
			err := <-randErr
			if err != nil {
				glog.Error(err)
			}
			continue
		}
	}
	summary.log()
}

func defaultAddr(addr, defaultHost, defaultPort string) string {
	if addr == "" {
		addr = defaultHost + ":" + defaultPort
	}
	if addr[0] == ':' {
		addr = defaultHost + addr
	}
	// not IPv6 safe
	if !strings.Contains(addr, ":") {
		addr = addr + ":" + defaultPort
	}
	if !strings.HasPrefix(addr, "http") {
		addr = "http://" + addr
	}

	return addr
}

func startEmbeddedStreamTester(ctx context.Context) {
	glog.Info("Starting embedded streamtester service")
	hostName, _ := os.Hostname()
	streamtesterMetrics.InitCensus(hostName, model.Version, "streamtester")
	s := server.NewStreamerServer(false, streamTesterLapiToken, streamTesterMistCreds, 4242)
	go func() {
		addr := fmt.Sprintf("%s:%s", "0.0.0.0", streamTesterPort)
		s.StartWebServer(ctx, addr)
	}()
}

func startEmbeddedBroadcaster(ctx context.Context, bCfg broadcasterConfig, presets string) {
	glog.Info("Starting embedded broadcaster service")

	// Increase Broadcaster timeouts
	common.SegUploadTimeoutMultiplier = 4.0
	common.SegmentUploadTimeout = 8 * time.Second
	common.HTTPDialTimeout = 8 * time.Second
	common.SegHttpPushTimeoutMultiplier = 4.0

	// Disable caching for Orchestrator Discovery Webhook
	common.WebhookDiscoveryRefreshInterval = 0

	// Start broadcaster
	cfg := starter.DefaultLivepeerConfig()
	cfg.Network = bCfg.network
	cfg.MaxSessions = intPointer(200)
	cfg.OrchWebhookURL = stringPointer(fmt.Sprintf("http://%s:%s/orchestrators", defaultHost, streamTesterPort))
	cfg.EthUrl = bCfg.ethUrl
	cfg.Datadir = bCfg.datadir
	cfg.Monitor = boolPointer(true)
	cfg.EthPassword = bCfg.ethPassword
	cfg.LocalVerify = boolPointer(false)
	cfg.HttpIngest = boolPointer(true)
	cfg.TranscodingOptions = &presets
	cfg.MaxTicketEV = bCfg.maxTicketEV
	cfg.MaxPricePerUnit = bCfg.maxPricePerUnit
	cfg.CliAddr = stringPointer(fmt.Sprintf("0.0.0.0:%s", bcastCliPort))
	cfg.Broadcaster = boolPointer(true)
	go starter.StartLivepeer(ctx, cfg)
}

func waitUntilBroadcasterIsReady(ctx context.Context, bcastHost string) {
	rCtx, _ := context.WithTimeout(ctx, bcastReadyTimeout)
	statusEndpoint := fmt.Sprintf("http://%s:%s/status", bcastHost, bcastCliPort)
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-rCtx.Done():
			glog.Error("Waiting for broadcaster timed out")
			return
		case <-ticker.C:
			resp, err := http.Get(statusEndpoint)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					return
				}
			}
		}
	}
}

type streamerClient struct {
	streamer          string
	leaderboardAddr   string
	leaderboardSecret string
	subgraph          string
	broadcaster       string
	metrics           promAPI.API
	client            *http.Client
	profiles          []string
}

func newStreamerClient(streamTesterURL, metricsURL, leaderboardAddr, leaderboardSecret, subgraph, broadcasterURL string, profiles []string) (*streamerClient, error) {
	client, err := promClient.NewClient(promClient.Config{
		Address: metricsURL,
	})
	if err != nil {
		return nil, err
	}

	return &streamerClient{
		client: &http.Client{
			Timeout: httpTimeout,
		},
		streamer:          streamTesterURL,
		metrics:           promAPI.NewAPI(client),
		leaderboardAddr:   leaderboardAddr,
		leaderboardSecret: leaderboardSecret,
		subgraph:          subgraph,
		broadcaster:       broadcasterURL,
		profiles:          profiles,
	}, nil
}

func (s *streamerClient) startStream(startStreamsReq *streamerModel.StartStreamsReq) (manifestID string, err error) {
	input, err := json.Marshal(startStreamsReq)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest("POST", s.streamer+"/start_streams", bytes.NewBuffer(input))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := s.client.Do(req)
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return "", err
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", errors.New(string(body))
	}

	var startStreamsRes streamerModel.StartStreamsRes
	if err := json.Unmarshal(body, &startStreamsRes); err != nil {
		return "", err
	}

	return startStreamsRes.BaseManifestID, nil
}

func (s *streamerClient) getFinishedStats(mid string) (*streamerModel.Stats, error) {
	var stats streamerModel.Stats

	for !stats.Finished {

		time.Sleep(5 * time.Second)

		req, err := http.NewRequest("GET", s.streamer+"/stats?base_manifest_id="+url.QueryEscape(mid), nil)
		if err != nil {
			return nil, err
		}

		res, err := s.client.Do(req)
		if err != nil {
			return nil, err
		}

		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			return nil, err
		}

		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, errors.New(string(body))
		}

		if err := json.Unmarshal(body, &stats); err != nil {
			return nil, err
		}
	}

	return &stats, nil
}

func avgMetric(streamer *streamerClient, embedded *broadcasterMetrics, metric string) float64 {
	if embedded != nil {
		return embedded.avg(metric)
	}

	query := fmt.Sprintf("sum(rate(livepeer_%s_sum[1m]))/sum(rate(livepeer_%s_count[1m]))", metric, metric)
	val, err := streamer.queryVectorMetric(query)
	if err != nil {
		glog.Error(err)
		return 0
	}
	valFloat := float64(0)
	if val.Len() > 0 {
		valFloat = float64((*val)[0].Value)
		if math.IsNaN(valFloat) {
			glog.Error(err)
			return 0
		}
	}
	return valFloat
}

func errorCount(s *streamerClient, metrics *broadcasterMetrics) []apiModels.Error {
	var errors map[string]int
	if metrics != nil {
		errors = metrics.incErrorCount()
	} else {
		res, err := s.queryErrorCounts()
		if err != nil {
			glog.Error(errors)
			return []apiModels.Error{}
		}
		errors = res
	}

	errArray := []apiModels.Error{}
	for errCode, count := range errors {
		errArray = append(errArray, apiModels.Error{
			ErrorCode: errCode,
			Count:     count,
		})
	}
	return errArray
}

func (s *streamerClient) queryErrorCounts() (map[string]int, error) {
	errors := make(map[string]int)
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	uploadErrs, _, err := s.metrics.Query(
		ctx,
		`sum(increase(livepeer_segment_source_upload_failed_total[1m])) by (error_code)`,
		time.Time{},
	)
	if err != nil {
		return nil, err
	}
	uploadErrsVec := uploadErrs.(promModels.Vector)
	for _, err := range uploadErrsVec {
		count := float64(err.Value)
		if count < 1 {
			continue
		}
		errors[string(err.Metric["error_code"])] += int(math.Round(count))
	}

	discoveryErrs, _, err := s.metrics.Query(
		ctx,
		"increase(livepeer_discovery_errors_total[1m])",
		time.Time{},
	)
	if err != nil {
		return nil, err
	}

	discoverErrsVec := discoveryErrs.(promModels.Vector)
	for _, err := range discoverErrsVec {
		count := float64(err.Value)
		if count < 1 {
			continue
		}
		errors[string(err.Metric["error_code"])] += int(math.Round(count))
	}

	transcodeErrs, _, err := s.metrics.Query(
		ctx,
		`sum(increase(livepeer_segment_transcode_failed_total[1m])) by (error_code)`,
		time.Time{},
	)
	if err != nil {
		return nil, err
	}
	transcodeErrsVec := transcodeErrs.(promModels.Vector)
	for _, err := range transcodeErrsVec {
		count := float64(err.Value)
		if count < 1 {
			continue
		}
		errors[string(err.Metric["error_code"])] += int(math.Round(count))
	}
	return errors, nil
}

func (s *streamerClient) queryVectorMetric(qry string) (*promModels.Vector, error) {
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	val, _, err := s.metrics.Query(ctx, qry, time.Time{})
	if err != nil {
		return &promModels.Vector{}, err
	}

	if val.Type().String() != "vector" {
		return &promModels.Vector{}, errors.New("result is not a valid vector")
	}

	vec := val.(promModels.Vector)
	return &vec, nil
}

func (s *streamerClient) postStats(stats *apiModels.Stats) error {
	input, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", s.leaderboardAddr+"/api/post_stats", bytes.NewBuffer(input))
	if err != nil {
		return err
	}

	hash := hmac.New(sha256.New, []byte(s.leaderboardSecret))
	hash.Write(input)
	req.Header.Set("Authorization", hex.EncodeToString(hash.Sum(nil)))
	req.Header.Set("Content-Type", "application/json")

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return err
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.New(string(body))
	}

	glog.Infof("Posted stats for orchestrator %s - success rate=%v   latency=%v", stats.Orchestrator, stats.SuccessRate, stats.RoundTripTime)
	return nil
}

type queryRes struct {
	Data struct {
		Transcoders []*orch
	}
}

type orch struct {
	Address    string `json:"id"`
	ServiceURI string `json:"serviceURI"`
}

func (s *streamerClient) getOrchestrators() ([]*orch, error) {
	query := map[string]string{
		"query": `
		{
			transcoders(where: {active: true}) {
			  id
				 serviceURI
			}
		  }
		`,
	}

	input, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", s.subgraph, bytes.NewBuffer(input))
	if err != nil {
		return nil, err
	}
	res, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, errors.New(string(body))
	}

	data := queryRes{}

	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}

	return data.Data.Transcoders, nil
}

// Get a random source/rendition sample segment and write it to external storage
func (s *streamerClient) randomSample(ctx context.Context, mid string, orch string) error {
	randSeqNo := rand.Int() % numSegments
	randProfile := s.profiles[rand.Int()%len(s.profiles)]
	//stream/manifestID/profile/SeqNo.ts
	sourceUrl := fmt.Sprintf("%v/stream/%v_0_0/source/%v.ts", s.broadcaster, mid, randSeqNo)
	renditionUrl := fmt.Sprintf("%v/stream/%v_0_0/%v/%v.ts", s.broadcaster, mid, randProfile, randSeqNo)
	var (
		rendition []byte
		source    []byte
		err       error
	)
	tick := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ctx.Done():
			glog.Info("random sampler timed out")
			// If we haven't found the segment try to sample from whatever playlist we can
			source, rendition, renditionUrl, err = s.sampleFromPlaylist(mid)
			if err != nil {
				glog.Error(err)
			}
			break
		case <-tick.C:
			rendition, err = s.downloadSegment(renditionUrl)
			if err != nil {
				glog.V(model.DEBUG).Info(err)
				continue
			}
			source, err = s.downloadSegment(sourceUrl)
			if err != nil {
				glog.V(model.DEBUG).Info(err)
				continue
			}
			break
		}
		break
	}

	if len(rendition) == 0 || len(source) == 0 {
		return fmt.Errorf("no segments found")
	}

	urlSplit := strings.Split(renditionUrl, "/")
	rendS := urlSplit[len(urlSplit)-2]
	fname := urlSplit[len(urlSplit)-1]

	rendF := fileName(orch, mid, rendS, fname)
	sourceF := fileName(orch, mid, "source", fname)

	src, _, err := testers.SaveToExternalStorage(sourceF, source)
	if err != nil {
		glog.Error(err)
		return err
	}
	glog.Infof("Wrote source segment to storage url=%v", src)

	rend, _, err := testers.SaveToExternalStorage(rendF, rendition)
	if err != nil {
		glog.Error(err)
		return err
	}
	glog.Infof("Wrote rendition to storage url=%v", rend)
	return nil
}

func baseFileName() string {
	y, m, d := start.Date()
	return fmt.Sprintf("%v-%v-%v", y, m, d)
}

func fileName(orch, mid, rend, fname string) string {
	return fmt.Sprintf("%v/%v/%v-%v-%v", baseFileName(), orch, mid, rend, fname)
}

func (s *streamerClient) sampleFromPlaylist(mid string) (source, rendition []byte, url string, err error) {
	mpl, err := s.downloadMasterPlaylist(mid)
	if err != nil {
		return nil, nil, "", err
	}

	sourcePlURI := fmt.Sprintf("%v_0_0/source.m3u8", mid)
	sourcePl, err := s.downloadMediaPlaylist(sourcePlURI)
	if err != nil {
		return nil, nil, "", err
	}

	// Filter source from variants
	variants := []*m3u8.Variant{}
	for _, v := range mpl.Variants {
		uriSplit := strings.Split(v.URI, "/")
		fName := uriSplit[len(uriSplit)-1]
		if strings.Contains(fName, "source") {
			continue
		}
		variants = append(variants, v)
	}

	if len(variants) == 0 {
		return nil, nil, "", fmt.Errorf("no transcoded renditions in playlist")
	}

	renditionPl, err := s.downloadMediaPlaylist(variants[rand.Int()%len(variants)].URI)
	if err != nil {
		return nil, nil, "", err
	}

	if sourcePl.Len() == 0 || renditionPl.Len() == 0 {
		return nil, nil, "", fmt.Errorf("no segments found")
	}

	r := rand.Int()
	segIdx := r % int(math.Min(float64(renditionPl.Len()), float64(sourcePl.Len())))
	rendURI := renditionPl.Segments[segIdx].URI
	urlSplit := strings.Split(rendURI, "/")
	fName := urlSplit[len(urlSplit)-1]

	rendition, err = s.downloadSegment(fmt.Sprintf("%v/%v", s.broadcaster, rendURI))
	if err != nil {
		return nil, nil, "", err
	}

	sourceURI := fmt.Sprintf("%v/stream/%v_0_0/source/%v", s.broadcaster, mid, fName)
	source, err = s.downloadSegment(sourceURI)
	if err != nil {
		return nil, nil, "", err
	}

	return source, rendition, rendURI, err
}

func (s *streamerClient) downloadSegment(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	res, err := s.client.Do(req)
	if err != nil {
		glog.Error(err)
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		glog.Error(err)
		return nil, err
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("%v", string(body))
	}
	return body, nil
}

func (s *streamerClient) downloadMasterPlaylist(manifestID string) (*m3u8.MasterPlaylist, error) {
	url := fmt.Sprintf("%v/stream/%v_0_0.m3u8", s.broadcaster, manifestID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	res, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, errors.New(string(body))
	}

	mpl := m3u8.NewMasterPlaylist()
	if err := mpl.Decode(*bytes.NewBuffer(body), true); err != nil {
		return nil, err
	}

	return mpl, nil
}

func (s *streamerClient) downloadMediaPlaylist(uri string) (*m3u8.MediaPlaylist, error) {
	url := fmt.Sprintf("%v/stream/%v", s.broadcaster, uri)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	res, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, errors.New(string(body))
	}

	gpl, plt, err := m3u8.Decode(*bytes.NewBuffer(body), true)
	if err != nil {
		return nil, err
	}
	if plt != m3u8.MEDIA {
		return nil, fmt.Errorf("Expecting media playlist, got %d", plt)
	}
	pl := gpl.(*m3u8.MediaPlaylist)
	return pl, nil
}

const (
	minSanityCheckSuccessRate       = 0.8
	maxSanityCheckRoundTripTime     = 2.0
	minSanityCheckOrchestratorCount = 20
)

type statsSummary struct {
	sanityCheckSuccessRateCount   int
	sanityCheckRoundTripTimeCount int
}

func (s *statsSummary) add(stats *apiModels.Stats) {
	if stats.SuccessRate >= minSanityCheckSuccessRate {
		s.sanityCheckSuccessRateCount++
	}
	if stats.RoundTripTime <= maxSanityCheckRoundTripTime {
		s.sanityCheckRoundTripTimeCount++
	}
}

func (s *statsSummary) log() {
	glog.Infof("Completed the orch-tester job, number of orchestrators with success rate higher than %v: %v, number of orchestrators with round trip time lower than %v: %v", minSanityCheckSuccessRate, s.sanityCheckSuccessRateCount, maxSanityCheckRoundTripTime, s.sanityCheckRoundTripTimeCount)
	if s.sanityCheckSuccessRateCount < minSanityCheckOrchestratorCount || s.sanityCheckRoundTripTimeCount < minSanityCheckOrchestratorCount {
		glog.Warning("Low number of orchestrators which passed the sanity check, please make sure that the orch-tester job is configured correctly")
	}
}

func boolPointer(b bool) *bool {
	return &b
}

func intPointer(i int) *int {
	return &i
}

func stringPointer(s string) *string {
	return &s
}
