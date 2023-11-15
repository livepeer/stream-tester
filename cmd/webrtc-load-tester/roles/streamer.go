package roles

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/cmd/webrtc-load-tester/utils"
)

type streamerArguments struct {
	BaseURL      string
	StreamKey    string
	InputFile    string
	TestDuration time.Duration
}

func Streamer() {
	var cliFlags = streamerArguments{}

	utils.ParseFlags(func(fs *flag.FlagSet) {
		fs.StringVar(&cliFlags.BaseURL, "base-url", "rtmp://rtmp.livepeer.com/live/", "Base URL for the RTMP endpoint to stream to")
		fs.StringVar(&cliFlags.StreamKey, "stream-key", "deadbeef", "Stream key to use for streaming")
		fs.StringVar(&cliFlags.InputFile, "input-file", "bbb_sunflower_1080p_30fps_2sGOP_noBframes_2min.mp4", "Input file to stream")
		fs.DurationVar(&cliFlags.TestDuration, "duration", 1*time.Minute, "How long to run the test")
	})

	ctx := utils.SignalContext()

	if err := runStreamerTest(ctx, cliFlags); err != nil {
		glog.Errorf("Error: %v\n", err)
		os.Exit(1)
	}
}

func runStreamerTest(ctx context.Context, args streamerArguments) error {
	url, err := buildRTMPURL(args.BaseURL, args.StreamKey)
	if err != nil {
		return err
	}

	file := args.InputFile
	if strings.HasPrefix(file, "http:") || strings.HasPrefix(file, "https:") {
		file, err = downloadFile(args.InputFile)
		if err != nil {
			return fmt.Errorf("failed to download file: %w", err)
		}
		defer os.Remove(file)
	}

	ctx, cancel := context.WithTimeout(ctx, args.TestDuration)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		"ffmpeg",
		"-re",
		"-stream_loop", "-1", // loop continuously until we stop the process
		"-i", file,
		"-c", "copy",
		"-f", "flv",
		url,
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start ffmpeg: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		if err.Error() == "signal: killed" {
			return nil
		}
		return fmt.Errorf("ffmpeg exited with error: %w", err)
	}

	return nil
}

func buildRTMPURL(baseURL, streamKey string) (string, error) {
	url, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	return url.JoinPath(streamKey).String(), nil
}

func downloadFile(url string) (string, error) {
	tempFile, err := os.CreateTemp("", "video-*.mp4")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	_, err = io.Copy(tempFile, resp.Body)
	if err != nil {
		return "", err
	}

	return tempFile.Name(), nil
}
