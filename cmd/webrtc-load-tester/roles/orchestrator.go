package roles

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/stream-tester/cmd/webrtc-load-tester/gcloud"
	"github.com/livepeer/stream-tester/cmd/webrtc-load-tester/utils"
)

const jobsPollingInterval = 1 * time.Minute

type loadTestArguments struct {
	TestID string // set one to recover a running test. auto-generated if not provided

	// Google Cloud
	GoogleCredentialsJSON string
	GoogleProjectID       string
	ContainerImage        string

	// Livepeer Studio
	APIServer string
	APIToken  string

	// Test config
	TestDuration time.Duration
	Streamer     struct {
		Region    string
		BaseURL   string
		InputFile string
	}
	Playback struct {
		BaseURL                string
		RegionViewersJSON      map[string]int
		ViewersPerWorker       int
		MemoryPerViewerMiB     int
		DelayBetweenRegions    time.Duration
		BaseScreenshotFolderOS *url.URL
		ScreenshotPeriod       time.Duration
	}
}

var (
	studioApi *api.Client
)

func Orchestrator() {
	var cliFlags = loadTestArguments{}

	utils.ParseFlags(func(fs *flag.FlagSet) {
		fs.StringVar(&cliFlags.TestID, "test-id", "", "ID of previous test to recover. If not provided, a new test will be started with a random ID")
		fs.StringVar(&cliFlags.GoogleCredentialsJSON, "google-credentials-json", "", "Google Cloud service account credentials JSON with access to Cloud Run")
		fs.StringVar(&cliFlags.GoogleProjectID, "google-project-id", "livepeer-test", "Google Cloud project ID")
		fs.StringVar(&cliFlags.ContainerImage, "container-image", "livepeer/webrtc-load-tester:master", "Container image to use for the worker jobs")

		fs.DurationVar(&cliFlags.TestDuration, "duration", 10*time.Minute, "How long to run the test")

		fs.StringVar(&cliFlags.Streamer.BaseURL, "streamer-base-url", "rtmp://rtmp.livepeer.monster/live/", "Base URL for the RTMP endpoint to stream to")
		fs.StringVar(&cliFlags.Streamer.Region, "streamer-region", "us-central1", "Region to run the streamer job in")
		fs.StringVar(&cliFlags.Streamer.InputFile, "streamer-input-file", "bbb_sunflower_1080p_30fps_2sGOP_noBframes_2min.mp4", "Input file to stream")

		fs.StringVar(&cliFlags.Playback.BaseURL, "playback-base-url", "https://monster.lvpr.tv/", "Base URL for the player page")
		utils.JSONVarFlag(fs, &cliFlags.Playback.RegionViewersJSON, "playback-region-viewers-json", `{"us-central1":100,"europe-west2":100}`, "JSON object of Google Cloud regions to the number of viewers that should be simulated there. Notice that the values must be multiples of playback-viewers-per-worker, and up to 1000 x that")
		fs.IntVar(&cliFlags.Playback.ViewersPerWorker, "playback-viewers-per-worker", 50, "Number of viewers to simulate per worker")
		fs.IntVar(&cliFlags.Playback.MemoryPerViewerMiB, "playback-memory-per-viewer-mib", 100, "Amount of memory to allocate per viewer (browser tab)")
		fs.DurationVar(&cliFlags.Playback.DelayBetweenRegions, "playback-delay-between-regions", 1*time.Minute, "How long to wait between starting jobs on different regions")
		utils.URLVarFlag(fs, &cliFlags.Playback.BaseScreenshotFolderOS, "playback-base-screenshot-folder-os", "", "Object Store URL for a folder where to save screenshots of the player. If unset, no screenshots will be taken")
		fs.DurationVar(&cliFlags.Playback.ScreenshotPeriod, "playback-screenshot-period", 1*time.Minute, "How often to take a screenshot of the player")

		fs.StringVar(&cliFlags.APIToken, "api-token", "", "Token of the Livepeer API to be used")
		fs.StringVar(&cliFlags.APIServer, "api-server", "livepeer.monster", "Server of the Livepeer API to be used")
	})

	initClients(cliFlags)

	defer func() {
		if err := recover(); err == context.Canceled {
			glog.Warning("Test cancelled")
		} else if err != nil {
			panic(err)
		}
	}()

	var err error
	ctx := utils.SignalContext()
	if cliFlags.TestID == "" {
		err = runLoadTest(ctx, cliFlags)
	} else {
		err = recoverLoadTest(ctx, cliFlags)
	}

	if err != nil {
		glog.Error("Error:", err)
		os.Exit(1)
	}
}

func initClients(cliFlags loadTestArguments) {
	studioApi = api.NewAPIClient(api.ClientOptions{Server: cliFlags.APIServer, AccessToken: cliFlags.APIToken})

	err := gcloud.InitClients(context.Background(), cliFlags.GoogleCredentialsJSON, cliFlags.GoogleProjectID)
	if err != nil {
		glog.Errorf("Error initializing cloud run client: %v", err)
		os.Exit(2)
	}

	totalViewers := 0
	for region, viewers := range cliFlags.Playback.RegionViewersJSON {
		unitSize := cliFlags.Playback.ViewersPerWorker
		max := 1000 * unitSize
		valid := viewers > 0 && viewers%unitSize == 0 && viewers <= max
		if !valid {
			glog.Errorf("Regional numbers of viewers must be multiples of %d and up to %d (%s=%d)", unitSize, max, region, viewers)
			os.Exit(2)
		}

		totalViewers += viewers
	}

	glog.Infof("Total number of viewers: %d\n", totalViewers)
}

func runLoadTest(ctx context.Context, args loadTestArguments) error {
	args.TestID = uuid.New().String()
	glog.Infof("Starting new test with ID %s", args.TestID)
	wait(ctx, 5*time.Second)

	stream, err := studioApi.CreateStream(api.CreateStreamReq{
		Name: "webrtc-load-test-" + args.TestID,
	})
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	glog.Infof("Stream created: %s", stream.ID)
	glog.Infof("Access the stream at: https://%s", path.Join(args.APIServer, "/dashboard/streams", stream.ID))

	_, streamer, err := gcloud.CreateJob(ctx, streamerJobSpec(args, stream.StreamKey))
	if err != nil {
		return fmt.Errorf("failed to create streamer job: %w", err)
	}
	defer gcloud.DeleteJob(args.Streamer.Region, streamer.Job)

	glog.Infof("Streamer job created on region %s: %s (execution: %s)", args.Streamer.Region, streamer.Job, streamer.Name)

	executions := []string{streamer.Name}
	for region, numViewers := range args.Playback.RegionViewersJSON {
		glog.Infof("Waiting %s before starting player in %s", args.Playback.DelayBetweenRegions, region)
		wait(ctx, args.Playback.DelayBetweenRegions)

		_, viewer, err := gcloud.CreateJob(ctx, playerJobSpec(args, region, numViewers, stream.PlaybackID))
		if err != nil {
			return fmt.Errorf("failed to create player job: %w", err)
		}
		defer gcloud.DeleteJob(region, viewer.Job)

		glog.Infof("Player job created on region %s: %s (execution: %s)", region, viewer.Job, viewer.Name)
		executions = append(executions, viewer.Name)
	}

	waitTestFinished(ctx, stream.ID, executions)

	return nil
}

func recoverLoadTest(ctx context.Context, args loadTestArguments) error {
	if args.TestID == "" {
		return fmt.Errorf("test-id must be provided to recover a test")
	}
	glog.Infof("Recovering test with ID %s", args.TestID)
	wait(ctx, 5*time.Second)

	// TODO: Find the stream by name using the testID

	var executions []string
	for region := range args.Playback.RegionViewersJSON {
		regionExecs, err := gcloud.ListExecutions(ctx, region, args.TestID)
		if err != nil {
			return fmt.Errorf("failed to list executions on region %s: %w", region, err)
		}

		ownedJobs := map[string]bool{}
		for _, exec := range regionExecs {
			executions = append(executions, exec.Name)

			if job := exec.Job; !ownedJobs[job] {
				ownedJobs[job] = true
				glog.Infof("Taking ownership of %s job on region %s", job, region)
				defer gcloud.DeleteJob(region, job)
			}
		}
	}

	waitTestFinished(ctx, "", executions)

	return nil
}

func waitTestFinished(ctx context.Context, streamID string, executions []string) {
	glog.Infof("Waiting for test to finish...")

	ticker := time.NewTicker(jobsPollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if streamID != "" {
			stream, err := studioApi.GetStream(streamID, false)
			if err != nil {
				glog.Errorf("Error getting stream status: %v\n", err)
				continue
			}

			glog.Infof("Stream status: lastSeen=%s sourceDurationSec=%.2f sourceSegments=%d transcodedSegments=%d",
				time.UnixMilli(stream.LastSeen).Format(time.RFC3339), stream.SourceSegmentsDuration, stream.SourceSegments, stream.TranscodedSegments)
		}

		allFinished := true
		for _, exec := range executions {
			finished := gcloud.CheckExecutionStatus(ctx, exec)
			allFinished = allFinished && finished
		}

		if allFinished {
			glog.Infof("Test finished")
			break
		}
	}
}

func streamerJobSpec(args loadTestArguments, streamKey string) gcloud.JobSpec {
	// Stream for a little longer since viewers join slowly
	additionalStreamDelay := args.Playback.DelayBetweenRegions * time.Duration(1+len(args.Playback.RegionViewersJSON))
	duration := args.TestDuration + additionalStreamDelay
	timeout := duration + 10*time.Minute

	return gcloud.JobSpec{
		Region: args.Streamer.Region,

		ContainerImage: args.ContainerImage,
		Role:           "streamer",
		Args: []string{
			"-base-url", args.Streamer.BaseURL,
			"-stream-key", streamKey,
			"-input-file", args.Streamer.InputFile,
			"-duration", duration.String(),
		},
		Timeout: timeout,

		TestID:    args.TestID,
		NumTasks:  1,
		CPUs:      1,
		MemoryMiB: 512,
	}
}

func playerJobSpec(args loadTestArguments, region string, viewers int, playbackID string) gcloud.JobSpec {
	simultaneous := args.Playback.ViewersPerWorker
	numTasks := viewers / args.Playback.ViewersPerWorker
	timeout := args.TestDuration + 10*time.Minute

	jobArgs := []string{
		"-base-url", args.Playback.BaseURL,
		"-playback-id", playbackID,
		// TODO: Support region/node-specific playback by building the playback URL here
		// "-playback-url", stream.PlaybackURL,
		"-simultaneous", strconv.Itoa(simultaneous),
		"-duration", args.TestDuration.String(),
	}
	if args.Playback.BaseScreenshotFolderOS != nil {
		jobArgs = append(jobArgs,
			"-screenshot-folder-os", args.Playback.BaseScreenshotFolderOS.JoinPath(args.TestID, region).String(),
			"-screenshot-period", args.Playback.ScreenshotPeriod.String(),
		)
	}

	return gcloud.JobSpec{
		Region: region,

		ContainerImage: args.ContainerImage,
		Role:           "player",
		Args:           jobArgs,
		Timeout:        timeout,

		TestID:    args.TestID,
		NumTasks:  numTasks,
		CPUs:      int(math.Ceil(float64(simultaneous) / 50)),                                       // 50 viewers per CPU
		MemoryMiB: int(math.Ceil(float64(simultaneous*args.Playback.MemoryPerViewerMiB)/512) * 512), // Round up to 512MB increments
	}
}

func wait(ctx context.Context, dur time.Duration) {
	select {
	case <-ctx.Done():
		panic(ctx.Err())
	case <-time.After(dur):
	}
}
