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
	Simultaenous uint
	TestDuration time.Duration
}

func Player() {
	var cliFlags = playerArguments{}

	parseFlags(func(fs *flag.FlagSet) {
		fs.StringVar(&cliFlags.BaseURL, "base-url", "https://lvpr.tv/", "Base URL for the player")
		fs.StringVar(&cliFlags.PlaybackID, "playback-id", "deadbeef", "Playback ID to use for the player")
		fs.UintVar(&cliFlags.Simultaenous, "simultaneous", 1, "How many players to run simultaneously")
		fs.DurationVar(&cliFlags.TestDuration, "duration", 1*time.Minute, "How long to run the test")
	})

	runPlayerTest(cliFlags)
}

func runPlayerTest(args playerArguments) {
	// Create a parent context to run a single browser instance
	ctx, cancel := chromedp.NewContext(
		context.Background(),
		chromedp.WithBrowserOption(
			chromedp.WithBrowserLogf(log.Printf),
			chromedp.WithBrowserErrorf(log.Printf),
		),
		chromedp.WithLogf(log.Printf),
		chromedp.WithErrorf(log.Printf),
	)
	defer cancel()

	// Browser is only started on the first Run call
	if err := chromedp.Run(ctx); err != nil {
		glog.Errorf("Error starting browser: %v\n", err)
		os.Exit(1)
	}

	errs := make(chan error, args.Simultaenous)
	for i := uint(0); i < args.Simultaenous; i++ {
		go func() {
			errs <- runSinglePlayerTest(ctx, args)
		}()
	}

	for i := uint(0); i < args.Simultaenous; i++ {
		err := <-errs
		if err != nil {
			glog.Errorf("Routine finished with error: %v\n", err)
		}
	}
}

func runSinglePlayerTest(ctx context.Context, args playerArguments) error {
	url, err := buildPlayerUrl(args.BaseURL, args.PlaybackID)
	if err != nil {
		return err
	}

	ctx, cancel := chromedp.NewContext(ctx)
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
