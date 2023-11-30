package roles

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/stream-tester/cmd/webrtc-load-tester/gcloud"
	"github.com/livepeer/stream-tester/cmd/webrtc-load-tester/utils"
)

const statusPollingInterval = 30 * time.Second

type loadTestArguments struct {
	TestID   string // set one to recover a running test. auto-generated if not provided
	StreamID string

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
		ManifestURL            string
		JWTPrivateKey          string
		RegionViewersJSON      map[string]int
		ViewersPerWorker       int
		MachineType            string
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
		fs.StringVar(&cliFlags.StreamID, "stream-id", "", "ID of existing stream to use. Notice that this will be used as the test ID as well but spawn new jobs instead of recovering existing ones")
		fs.StringVar(&cliFlags.GoogleCredentialsJSON, "google-credentials-json", "", "Google Cloud service account credentials JSON with access to Cloud Run")
		fs.StringVar(&cliFlags.GoogleProjectID, "google-project-id", "livepeer-test", "Google Cloud project ID")
		fs.StringVar(&cliFlags.ContainerImage, "container-image", "livepeer/webrtc-load-tester:master", "Container image to use for the worker jobs")

		fs.DurationVar(&cliFlags.TestDuration, "duration", 10*time.Minute, "How long to run the test")

		fs.StringVar(&cliFlags.Streamer.BaseURL, "streamer-base-url", "rtmp://rtmp.livepeer.monster/live/", "Base URL for the RTMP endpoint to stream to")
		fs.StringVar(&cliFlags.Streamer.Region, "streamer-region", "us-central1", "Region to run the streamer job in")
		fs.StringVar(&cliFlags.Streamer.InputFile, "streamer-input-file", "bbb_sunflower_1080p_30fps_2sGOP_noBframes_2min.mp4", "Input file to stream")

		fs.StringVar(&cliFlags.Playback.BaseURL, "playback-base-url", "https://monster.lvpr.tv/", "Base URL for the player page")
		fs.StringVar(&cliFlags.Playback.ManifestURL, "playback-manifest-url", "", "URL for playback")
		fs.StringVar(&cliFlags.Playback.JWTPrivateKey, "playback-jwt-private-key", "", "Private key to sign JWT tokens for access controlled playback")
		utils.JSONVarFlag(fs, &cliFlags.Playback.RegionViewersJSON, "playback-region-viewers-json", `{"us-central1":100,"europe-west2":100}`, "JSON object of Google Cloud regions to the number of viewers that should be simulated there. Notice that the values must be multiples of playback-viewers-per-worker, and up to 1000 x that")
		fs.IntVar(&cliFlags.Playback.ViewersPerWorker, "playback-viewers-per-worker", 10, "Number of viewers to simulate per worker")
		fs.StringVar(&cliFlags.Playback.MachineType, "playback-machine-type", "n2-highcpu-4", "Machine type to use for player jobs")
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

func runLoadTest(ctx context.Context, args loadTestArguments) (err error) {
	var stream *api.Stream
	if args.StreamID != "" {
		stream, err = studioApi.GetStream(args.StreamID, false)
		if err != nil {
			return fmt.Errorf("failed to retrieve stream: %w", err)
		}
		glog.Infof("Retrieved stream with name: %s", stream.Name)
	} else {
		var playbackPolicy *api.PlaybackPolicy
		if args.Playback.JWTPrivateKey != "" {
			playbackPolicy = &api.PlaybackPolicy{Type: "jwt"}
		}

		stream, err = studioApi.CreateStream(api.CreateStreamReq{
			Name:           "load-test-" + time.Now().UTC().Format(time.RFC3339),
			PlaybackPolicy: playbackPolicy,
		})
		if err != nil {
			return fmt.Errorf("failed to create stream: %w", err)
		}
		glog.Infof("Stream created: %s", stream.ID)
	}

	// Use the stream ID as the test ID for simplicity. Helps on recovering a running test as well.
	args.TestID = stream.ID
	glog.Infof("Starting new test with ID %s", args.TestID)
	wait(ctx, 5*time.Second)

	glog.Infof("Access the stream at: https://%s", path.Join(args.APIServer, "/dashboard/streams", stream.ID))

	streamerSpec := streamerVMSpec(args, stream.StreamKey)
	streamerTemplateURL, streamerTemplateName, err := gcloud.CreateVMTemplate(ctx, streamerVMSpec(args, stream.StreamKey))
	if err != nil {
		return fmt.Errorf("failed to create streamer VM template: %w", err)
	}
	defer gcloud.DeleteVMTemplate(streamerTemplateName)

	playerSpec := playerVMSpec(args, stream.PlaybackID)
	playerTemplateURL, playerTemplateName, err := gcloud.CreateVMTemplate(ctx, playerSpec)
	if err != nil {
		return fmt.Errorf("failed to create player VM template: %w", err)
	}
	defer gcloud.DeleteVMTemplate(playerTemplateName)

	var createdVMGroups []gcloud.VMGroupInfo
	defer func() { gcloud.DeleteVMGroups(createdVMGroups) }()

	streamerGroup, err := gcloud.CreateVMGroup(ctx, streamerSpec, streamerTemplateURL, args.Streamer.Region, 1)
	if err != nil {
		return fmt.Errorf("failed to create streamer VM group: %w", err)
	}
	createdVMGroups = append(createdVMGroups, gcloud.VMGroupInfo{args.Streamer.Region, streamerGroup})

	glog.Infof("Streamer VM created on region %s: %s", args.Streamer.Region, streamerGroup)

	for region, numViewers := range args.Playback.RegionViewersJSON {
		glog.Infof("Waiting %s before starting player in %s", args.Playback.DelayBetweenRegions, region)
		wait(ctx, args.Playback.DelayBetweenRegions)

		numInstances := int64(numViewers / args.Playback.ViewersPerWorker)
		viewerGroup, err := gcloud.CreateVMGroup(ctx, playerSpec, playerTemplateURL, region, numInstances)
		if err != nil {
			return fmt.Errorf("failed to create player VM group in %s: %w", region, err)
		}
		createdVMGroups = append(createdVMGroups, gcloud.VMGroupInfo{region, viewerGroup})

		glog.Infof("Player VM group created on region %s: %s", region, viewerGroup)
	}

	waitTestFinished(ctx, stream.ID, createdVMGroups, false)

	return nil
}

func recoverLoadTest(ctx context.Context, args loadTestArguments) error {
	if args.TestID == "" {
		return fmt.Errorf("test-id must be provided to recover a test")
	}
	glog.Infof("Recovering test with ID %s", args.TestID)
	wait(ctx, 5*time.Second)

	templates, err := gcloud.ListVMTemplates(ctx, args.TestID)
	if err != nil {
		return fmt.Errorf("failed to list VM templates: %w", err)
	}
	for _, template := range templates {
		defer gcloud.DeleteVMTemplate(template)
	}

	var vmGroups []gcloud.VMGroupInfo
	defer func() { gcloud.DeleteVMGroups(vmGroups) }()

	for region := range args.Playback.RegionViewersJSON {
		regionGroups, err := gcloud.ListVMGroups(ctx, region, args.TestID)
		if err != nil {
			return fmt.Errorf("failed to list executions on region %s: %w", region, err)
		}

		for _, group := range regionGroups {
			vmGroups = append(vmGroups, gcloud.VMGroupInfo{region, group})
		}
	}

	waitTestFinished(ctx, args.TestID, vmGroups, true)

	return nil
}

func waitTestFinished(ctx context.Context, streamID string, vmGroups []gcloud.VMGroupInfo, isRecover bool) {
	glog.Infof("Waiting for test to finish...")

	ticker := time.NewTicker(statusPollingInterval)
	defer ticker.Stop()

	// assume the stream was already active if we're recovering a running test
	streamWasActive := isRecover
	for {
		stream, err := studioApi.GetStream(streamID, false)
		if err != nil {
			glog.Errorf("Error getting stream status: %v\n", err)
			continue
		}
		streamWasActive = streamWasActive || stream.IsActive

		glog.Infof("Stream status: isActive=%v lastSeen=%s sourceDurationSec=%.2f sourceSegments=%d transcodedSegments=%d",
			stream.IsActive, time.UnixMilli(stream.LastSeen).Format(time.RFC3339), stream.SourceSegmentsDuration, stream.SourceSegments, stream.TranscodedSegments)

		for _, group := range vmGroups {
			err := gcloud.CheckVMGroupStatus(ctx, group.Region, group.Name)
			if err != nil {
				glog.Errorf("Error checking VM group status: %v\n", err)
			}
		}

		// we consider the test finished if we saw the stream active at least
		// once and then it went inactive.
		if streamWasActive && !stream.IsActive {
			glog.Infof("Test finished")
			break
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func streamerVMSpec(args loadTestArguments, streamKey string) gcloud.VMTemplateSpec {
	// Stream for a little longer since viewers join slowly
	additionalStreamDelay := args.Playback.DelayBetweenRegions * time.Duration(1+len(args.Playback.RegionViewersJSON))
	duration := args.TestDuration + additionalStreamDelay

	return gcloud.VMTemplateSpec{
		ContainerImage: args.ContainerImage,
		Role:           "streamer",
		Args: []string{
			"-base-url", args.Streamer.BaseURL,
			"-stream-key", streamKey,
			"-input-file", args.Streamer.InputFile,
			"-duration", duration.String(),
		},

		TestID:      args.TestID,
		MachineType: "n1-standard-1",
	}
}

func playerVMSpec(args loadTestArguments, playbackID string) gcloud.VMTemplateSpec {
	simultaneous := args.Playback.ViewersPerWorker
	// numTasks := viewers / args.Playback.ViewersPerWorker

	playbackURL := ""
	if args.Playback.ManifestURL != "" {
		playbackURL = fmt.Sprintf(args.Playback.ManifestURL, playbackID)
	}

	jobArgs := []string{
		"-base-url", args.Playback.BaseURL,
		"-playback-id", playbackID,
		"-playback-url", playbackURL,
		"-jwt-private-key", args.Playback.JWTPrivateKey,
		"-simultaneous", strconv.Itoa(simultaneous),
		"-duration", args.TestDuration.String(),
	}
	if args.Playback.BaseScreenshotFolderOS != nil {
		jobArgs = append(jobArgs,
			"-screenshot-folder-os", args.Playback.BaseScreenshotFolderOS.JoinPath(args.TestID).String(),
			"-screenshot-period", args.Playback.ScreenshotPeriod.String(),
		)
	}

	return gcloud.VMTemplateSpec{
		ContainerImage: args.ContainerImage,
		Role:           "player",
		Args:           jobArgs,

		TestID:      args.TestID,
		MachineType: args.Playback.MachineType,
	}
}

func wait(ctx context.Context, dur time.Duration) {
	select {
	case <-ctx.Done():
		panic(ctx.Err())
	case <-time.After(dur):
	}
}
