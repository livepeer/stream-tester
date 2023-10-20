package roles

import (
	"context"
	"flag"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/golang/glog"
)

type playerArguments struct {
	BaseURL      string
	PlaybackID   string
	TestDuration time.Duration
}

func Player() {
	var cliFlags = playerArguments{}

	parseFlags(func(fs *flag.FlagSet) {
		fs.StringVar(&cliFlags.BaseURL, "base-url", "https://lvpr.tv/", "Base URL for the player")
		fs.StringVar(&cliFlags.PlaybackID, "playback-id", "deadbeef", "Playback ID to use for the player")
		fs.DurationVar(&cliFlags.TestDuration, "duration", 1*time.Minute, "How long to run the test")
	})

	if err := runPlayerTest(cliFlags); err != nil {
		glog.Errorf("Error: %v\n", err)
		os.Exit(1)
	}
}

func runPlayerTest(args playerArguments) error {
	url, err := buildPlayerUrl(args.BaseURL, args.PlaybackID)
	if err != nil {
		return err
	}

	ctx, cancel := chromedp.NewContext(
		context.Background(),
		chromedp.WithBrowserOption(
			chromedp.WithBrowserLogf(log.Printf),
			chromedp.WithBrowserErrorf(log.Printf),
		),
		// chromedp.WithDebugf(log.Printf),
	)
	defer cancel()

	tasks := chromedp.Tasks{
		chromedp.Navigate(url),
		chromedp.Sleep(args.TestDuration),
	}
	return chromedp.Run(ctx, tasks)
}

func buildPlayerUrl(baseURL, playbackID string) (string, error) {
	url, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	query := url.Query()
	query.Set("v", playbackID)
	// force player to only use WebRTC playback
	query.Set("lowLatency", "force")
	url.RawQuery = query.Encode()

	return url.String(), nil
}
