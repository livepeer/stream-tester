package roles

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/golang/glog"
	"github.com/livepeer/go-tools/drivers"
	"github.com/livepeer/stream-tester/cmd/webrtc-load-tester/utils"
)

const uploadScreenshotTimeout = 10 * time.Second

type playerArguments struct {
	BaseURL                 string
	PlaybackID, PlaybackURL string // only one will be used, playbackURL takes precedence
	Simultaenous            uint
	TestDuration            time.Duration

	ScreenshotFolderOS string
	ScreenshotPeriod   time.Duration
}

func Player() {
	var cliFlags = playerArguments{}

	utils.ParseFlags(func(fs *flag.FlagSet) {
		fs.StringVar(&cliFlags.BaseURL, "base-url", "https://lvpr.tv/", "Base URL for the player")
		fs.StringVar(&cliFlags.PlaybackID, "playback-id", "", "Playback ID to use for the player")
		fs.StringVar(&cliFlags.PlaybackURL, "playback-url", "", "Playback URL to use for the player. Will override any playback-id value")
		fs.UintVar(&cliFlags.Simultaenous, "simultaneous", 1, "How many players to run simultaneously")
		fs.DurationVar(&cliFlags.TestDuration, "duration", 1*time.Minute, "How long to run the test")
		fs.StringVar(&cliFlags.ScreenshotFolderOS, "screenshot-folder-os", "", "Object Store URL for a folder where to save screenshots of the player. If unset, no screenshots will be taken")
		fs.DurationVar(&cliFlags.ScreenshotPeriod, "screenshot-period", 1*time.Minute, "How often to take a screenshot of the player")
	})

	if cliFlags.PlaybackID == "" && cliFlags.PlaybackURL == "" {
		glog.Fatal("Either playback-id or playback-url must be provided")
	}

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
		i := i // avoid go's loop variable capture
		go func() {
			errs <- runSinglePlayerTest(ctx, args, i)
		}()
	}

	for i := uint(0); i < args.Simultaenous; i++ {
		err := <-errs
		if err != nil {
			glog.Errorf("Routine finished with error: %v\n", err)
		}
	}
}

func runSinglePlayerTest(ctx context.Context, args playerArguments, idx uint) error {
	url, err := buildPlayerUrl(args.BaseURL, args.PlaybackID, args.PlaybackURL)
	if err != nil {
		return err
	}

	ctx, cancel := chromedp.NewContext(ctx)
	defer cancel()

	tasks := chromedp.Tasks{
		chromedp.Navigate(url),
	}

	if args.ScreenshotFolderOS == "" {
		tasks = append(tasks, chromedp.Sleep(args.TestDuration))
	} else {
		osFolder := args.ScreenshotFolderOS
		// make sure the folder ends with a slash since NewSession just does a string concat
		if !strings.HasSuffix(osFolder, "/") {
			osFolder += "/"
		}

		driver, err := drivers.ParseOSURL(osFolder, true)
		if err != nil {
			return err
		}
		storage := driver.NewSession(fmt.Sprintf("player-%d", idx))

		// grab an initial screenshot
		tasks = append(tasks, uploadScreenshot(storage, "screenshot-000-00:00:00.jpg"))

		numPics := int(args.TestDuration / args.ScreenshotPeriod)
		for picIdx := 1; picIdx <= numPics; picIdx++ {
			tasks = append(tasks, chromedp.Sleep(args.ScreenshotPeriod))

			screenshotTime := time.Duration(picIdx) * args.ScreenshotPeriod
			name := fmt.Sprintf("screenshot-%03d-%s.jpg", picIdx, formatDuration(screenshotTime))
			tasks = append(tasks, uploadScreenshot(storage, name))
		}

		if remaining := args.TestDuration % args.ScreenshotPeriod; remaining != 0 {
			tasks = append(tasks, chromedp.Sleep(remaining))
			name := fmt.Sprintf("screenshot-%03d-%s.jpg", numPics+1, formatDuration(args.TestDuration))
			tasks = append(tasks, uploadScreenshot(storage, name))
		}
	}

	return chromedp.Run(ctx, tasks)
}

func uploadScreenshot(storage drivers.OSSession, name string) chromedp.ActionFunc {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		var picBuf []byte
		screenshotAction := chromedp.FullScreenshot(&picBuf, 90)

		if err := screenshotAction.Do(ctx); err != nil {
			return err
		}

		go func() {
			_, err := storage.SaveData(ctx, name, bytes.NewBuffer(picBuf), nil, uploadScreenshotTimeout)
			if err != nil {
				glog.Errorf("Error uploading screenshot: %v\n", err)
			}
		}()
		return nil
	})
}

func formatDuration(d time.Duration) string {
	return time.Time{}.Add(d).Format(time.TimeOnly)
}

func buildPlayerUrl(baseURL, playbackID, playbackURL string) (string, error) {
	url, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	query := url.Query()
	if playbackURL != "" {
		query.Set("url", playbackURL)
	} else {
		query.Set("v", playbackID)
	}
	// force player to only use WebRTC playback
	query.Set("lowLatency", "force")
	url.RawQuery = query.Encode()

	return url.String(), nil
}
