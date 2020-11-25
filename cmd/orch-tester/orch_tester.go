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
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	apiModels "github.com/livepeer/leaderboard-serverless/models"
	streamerModel "github.com/livepeer/stream-tester/model"
	promClient "github.com/prometheus/client_golang/api"
	promAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	promModels "github.com/prometheus/common/model"
)

const streamTesterPort = "7934"
const prometheusPort = "9090"
const bcastMediaPort = "8935"
const bcastRTMPPort = "1935"
const defaultBcast = "127.0.0.1"
const httpTimeout = 8 * time.Second

func main() {
	flag.Set("logtostderr", "true")
	region := flag.String("region", "", "Region this service is operating in")
	streamTester := flag.String("streamtester", "127.0.0.1"+":"+streamTesterPort, "Address for stream-tester server instance")
	broadcaster := flag.String("broadcaster", "127.0.0.1", "Broadcaster address")
	metrics := flag.String("metrics", "127.0.0.1"+":"+prometheusPort, "Broadcaster metrics port")
	media := flag.String("media", bcastMediaPort, "Broadcaster HTTP port")
	rtmp := flag.String("rtmp", bcastRTMPPort, "broadcaster RTMP port")
	leaderboard := flag.String("leaderboard", "127.0.0.1:3001", "HTTP Address of the serverless leadearboard API")
	leaderboardSecret := flag.String("leaderboard-secret", "", "Secret for the Leaderboard API")
	subgraph := flag.String("subgraph", "https://api.thegraph.com/subgraphs/name/livepeer/livepeer-canary", "Livepeer subgraph URL")
	// Video config
	videoFile := flag.String("video", "official_test_source_2s_keys_24pfs_30s.mp4", "video file to use, has to be present in stream-tester root")
	numProfiles := flag.Int("profiles", 3, "number of video profiles to use on the broadcaster")
	repeat := flag.Int("repeat", 1, "number of times to repeat the stream")
	simultaneous := flag.Int("simultaneous", 1, "number of times to run the stream simultaneously")
	flag.Parse()

	if *region == "" {
		log.Fatal("region is required")
	}

	metricsURL, err := defaultAddr(*metrics, "127.0.0.1", prometheusPort)
	if err != nil {
		log.Fatal(err)
	}

	streamTesterURL, err := defaultAddr(*streamTester, "127.0.0.1", streamTesterPort)
	if err != nil {
		log.Fatal(err)
	}

	leaderboardURL, err := defaultAddr(*leaderboard, "127.0.0.1", "3001")
	if err != nil {
		log.Fatal(err)
	}

	subgraphURL, err := defaultAddr(*subgraph, "127.0.0.1", "8080")
	if err != nil {
		log.Fatal(err)
	}

	rtmpUint, err := strconv.ParseUint(*rtmp, 10, 16)
	if err != nil {
		log.Fatal(err)
	}

	mediaUint, err := strconv.ParseUint(*media, 10, 16)
	if err != nil {
		log.Fatal(err)
	}

	streamer, err := newStreamerClient(streamTesterURL, metricsURL, leaderboardURL, *leaderboardSecret, subgraphURL)
	if err != nil {
		log.Fatal(err)
	}

	orchestrators, err := streamer.getOrchestrators()
	if err != nil {
		log.Fatal(err)
	}

	refreshWait := 90 * time.Second

	for _, o := range orchestrators {
		time.Sleep(refreshWait)

		req := &streamerModel.StartStreamsReq{
			Host:            *broadcaster,
			RTMP:            uint16(rtmpUint),
			Media:           uint16(mediaUint),
			Repeat:          uint(*repeat),
			Simultaneous:    uint(*simultaneous),
			Orchestrators:   []string{o.ServiceURI},
			ProfilesNum:     *numProfiles,
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

		glog.Infof("Started stream for orchestrator %v", o.Address)

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
		apiStats.SuccessRate = (float64(apiStats.SegmentsReceived) / float64(*numProfiles) / float64(apiStats.SegmentsSent)) * 100

		avgSegDuration, err := streamer.avgSegDuration()
		if err != nil {
			glog.Error(err)
		}
		apiStats.AvgSegDuration = avgSegDuration

		avgRoundTripTime, err := streamer.avgRoundTripTime()
		if err != nil {
			glog.Error(err)
		}
		apiStats.AvgRoundTripTime = avgRoundTripTime
		if avgRoundTripTime > 0 {
			apiStats.AvgRoundTripScore = avgSegDuration / avgRoundTripTime
		}

		avgUploadTime, err := streamer.avgUploadTime()
		if err != nil {
			glog.Error(err)
		}
		apiStats.AvgUploadTime = avgUploadTime
		if avgUploadTime > 0 {
			apiStats.AvgUploadScore = avgSegDuration / avgUploadTime
		}

		avgDownloadTime, err := streamer.avgDownloadTime()
		if err != nil {
			glog.Error(err)
		}
		apiStats.AvgDownloadTime = avgDownloadTime
		if avgDownloadTime > 0 {
			apiStats.AvgDownloadScore = avgSegDuration / avgDownloadTime
		}

		transcodeTime := avgRoundTripTime - avgUploadTime - avgDownloadTime
		if transcodeTime > 0 {
			apiStats.AvgTranscodeTime = transcodeTime
			apiStats.AvgTranscodeScore = avgSegDuration / transcodeTime
			apiStats.TotalScore = apiStats.AvgRoundTripScore * apiStats.SuccessRate / 100
		}

		errors, err := streamer.queryErrorCounts()
		if err != nil {
			glog.Error(err)
		}
		apiStats.Errors = errors

		if err := streamer.postStats(apiStats); err != nil {
			glog.Error(err)
			continue
		}
	}
}

func validateURL(addr string) (string, error) {
	url, err := url.ParseRequestURI(addr)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}

func defaultAddr(addr, defaultHost, defaultPort string) (string, error) {
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

	return addr, nil
}

type streamerClient struct {
	streamer          string
	leaderboardAddr   string
	leaderboardSecret string
	subgraph          string
	metrics           promAPI.API
	client            *http.Client
}

func newStreamerClient(streamTesterURL, metricsURL, leaderboardAddr, leaderboardSecret, subgraph string) (*streamerClient, error) {
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

func (s *streamerClient) avgSegDuration() (float64, error) {
	val, err := s.queryVectorMetric(
		"rate(livepeer_source_segment_duration_seconds_sum[1m])/rate(livepeer_source_segment_duration_seconds_count[1m])",
	)
	if err != nil {
		return 0, err
	}
	valFloat := float64(0)
	if val.Len() > 0 {
		valFloat = float64((*val)[0].Value)
		if math.IsNaN(valFloat) {
			return 0, nil
		}
	}
	return valFloat, nil
}

func (s *streamerClient) avgUploadTime() (float64, error) {
	val, err := s.queryVectorMetric(
		"rate(livepeer_upload_time_seconds_sum[1m])/rate(livepeer_upload_time_seconds_count[1m])",
	)
	if err != nil {
		return 0, err
	}
	valFloat := float64(0)
	if val.Len() > 0 {
		valFloat = float64((*val)[0].Value)
		if math.IsNaN(valFloat) {
			return 0, nil
		}
	}
	return valFloat, nil
}

func (s *streamerClient) avgDownloadTime() (float64, error) {
	val, err := s.queryVectorMetric(
		"rate(livepeer_download_time_seconds_sum[1m])/rate(livepeer_download_time_seconds_count[1m])",
	)
	if err != nil {
		return 0, err
	}
	valFloat := float64(0)
	if val.Len() > 0 {
		valFloat = float64((*val)[0].Value)
		if math.IsNaN(valFloat) {
			return 0, nil
		}
	}
	return valFloat, nil
}

func (s *streamerClient) avgRoundTripTime() (float64, error) {
	val, err := s.queryVectorMetric(
		"rate(livepeer_transcode_overall_latency_seconds_sum[1m])/rate(livepeer_transcode_overall_latency_seconds_count[1m])",
	)
	if err != nil {
		return 0, err
	}
	valFloat := float64(0)
	if val.Len() > 0 {
		valFloat = float64((*val)[0].Value)
		if math.IsNaN(valFloat) {
			return 0, nil
		}
	}
	return valFloat, nil
}

func (s *streamerClient) queryErrorCounts() ([]apiModels.Error, error) {
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

	errArray := []apiModels.Error{}
	for errCode, count := range errors {
		errArray = append(errArray, apiModels.Error{
			ErrorCode: errCode,
			Count:     count,
		})
	}
	return errArray, nil
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

	glog.Infof("Posted stats for orchestrator %s - success rate=%v   score=%v", stats.Orchestrator, stats.SuccessRate, stats.TotalScore)
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
