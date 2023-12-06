package roles

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/golang-jwt/jwt/v4"
	"github.com/golang/glog"
	"github.com/livepeer/go-tools/drivers"
	"github.com/livepeer/stream-tester/cmd/webrtc-load-tester/utils"
)

const uploadScreenshotTimeout = 10 * time.Second

type playerArguments struct {
	BaseURL                 string
	PlaybackID, PlaybackURL string // only one will be used, playbackURL takes precedence
	JWTPrivateKey           string
	Simultaneous            uint
	PlayerStartInterval     time.Duration
	TestDuration            time.Duration
	Headless                bool

	ScreenshotFolderOS *url.URL
	ScreenshotPeriod   time.Duration
}

func Player() {
	var cliFlags = playerArguments{}

	utils.ParseFlags(func(fs *flag.FlagSet) {
		fs.StringVar(&cliFlags.BaseURL, "base-url", "https://lvpr.tv/", "Base URL for the player")
		fs.StringVar(&cliFlags.PlaybackID, "playback-id", "", "Playback ID to use for the player")
		fs.StringVar(&cliFlags.PlaybackURL, "playback-url", "", "Playback URL to use for the player. Will override any playback-id value")
		fs.StringVar(&cliFlags.JWTPrivateKey, "jwt-private-key", "", "Private key to use for signing JWT tokens in access controlled playback")
		fs.UintVar(&cliFlags.Simultaneous, "simultaneous", 1, "How many players to run simultaneously")
		fs.DurationVar(&cliFlags.PlayerStartInterval, "player-start-interval", 2*time.Second, "How often to wait between starting the simultaneous players")
		fs.DurationVar(&cliFlags.TestDuration, "duration", 1*time.Minute, "How long to run the test")
		utils.URLVarFlag(fs, &cliFlags.ScreenshotFolderOS, "screenshot-folder-os", "", "Object Store URL for a folder where to save screenshots of the player. If unset, no screenshots will be taken")
		fs.DurationVar(&cliFlags.ScreenshotPeriod, "screenshot-period", 1*time.Minute, "How often to take a screenshot of the player")
		fs.BoolVar(&cliFlags.Headless, "headless", true, "Run Chrome in headless mode (no-GUI)")
	})

	if cliFlags.PlaybackID == "" && cliFlags.PlaybackURL == "" {
		glog.Fatal("Either playback-id or playback-url must be provided")
	}

	runPlayerTest(cliFlags)
}

func runPlayerTest(args playerArguments) {
	// Create a parent context to run a single browser instance
	opts := chromedp.DefaultExecAllocatorOptions[:]
	if !args.Headless {
		opts = append(opts,
			chromedp.Flag("headless", false),
			chromedp.Flag("hide-scrollbars", false),
			chromedp.Flag("mute-audio", false),
		)
	}
	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancel()
	ctx, cancel := chromedp.NewContext(
		allocCtx,
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

	errs := make(chan error, args.Simultaneous)
	for i := uint(0); i < args.Simultaneous; i++ {
		i := i // avoid go's loop variable capture

		if args.PlayerStartInterval > 0 {
			time.Sleep(args.PlayerStartInterval)
		}

		go func() {
			errs <- runSinglePlayerTest(ctx, args, i)
		}()
	}

	for i := uint(0); i < args.Simultaneous; i++ {
		err := <-errs
		if err != nil {
			glog.Errorf("Routine finished with error: %v\n", err)
		}
	}
}

func runSinglePlayerTest(ctx context.Context, args playerArguments, idx uint) error {
	playbackURL := args.PlaybackURL
	if args.JWTPrivateKey != "" {
		if playbackURL == "" {
			return fmt.Errorf("playback-url must be provided when using JWT")
		}

		jwt, err := signJwt(args.PlaybackID, args.JWTPrivateKey, 2*args.TestDuration)
		if err != nil {
			return err
		}
		playbackURL = addQuery(playbackURL, "jwt", jwt)
	}

	url, err := buildPlayerUrl(args.BaseURL, args.PlaybackID, playbackURL)
	if err != nil {
		return err
	}

	ctx, cancel := chromedp.NewContext(ctx)
	defer cancel()

	tasks := chromedp.Tasks{
		chromedp.Navigate(url),
	}

	if args.ScreenshotFolderOS == nil {
		tasks = append(tasks, chromedp.Sleep(args.TestDuration))
	} else {
		osFolder := args.ScreenshotFolderOS
		if region := getRegion(); region != "" {
			osFolder = osFolder.JoinPath(region)
		}
		osFolder = osFolder.JoinPath(getHostname(), fmt.Sprintf("player-%d", idx))

		driver, err := drivers.ParseOSURL(osFolder.String(), true)
		if err != nil {
			return err
		}
		storage := driver.NewSession("")

		// grab an initial screenshot
		tasks = append(tasks, uploadScreenshot(storage, screenshotName(0, 0)))

		numPics := int(args.TestDuration / args.ScreenshotPeriod)
		for picIdx := 1; picIdx <= numPics; picIdx++ {
			tasks = append(tasks, chromedp.Sleep(args.ScreenshotPeriod))

			screenshotTime := time.Duration(picIdx) * args.ScreenshotPeriod
			name := screenshotName(picIdx, screenshotTime)
			tasks = append(tasks, uploadScreenshot(storage, name))
		}

		if remaining := args.TestDuration % args.ScreenshotPeriod; remaining != 0 {
			tasks = append(tasks, chromedp.Sleep(remaining))
			name := screenshotName(numPics+1, args.TestDuration)
			tasks = append(tasks, uploadScreenshot(storage, name))
		}
	}

	return chromedp.Run(ctx, tasks)
}

func uploadScreenshot(storage drivers.OSSession, name string) chromedp.ActionFunc {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		var picBuf []byte
		screenshotAction := chromedp.FullScreenshot(&picBuf, 50)

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

func screenshotName(idx int, d time.Duration) string {
	timeStr := time.Time{}.Add(d).Format("15h04m05s")
	return fmt.Sprintf("screenshot-%03d-%s.jpg", idx, timeStr)
}

func buildPlayerUrl(baseURL, playbackID, playbackURL string) (string, error) {
	url := baseURL

	if playbackURL != "" {
		url = addQuery(url, "url", playbackURL)
	} else {
		url = addQuery(url, "v", playbackID)
	}
	// Force player to only use WebRTC playback by default, but allow base URL to override it
	if !strings.Contains(url, "lowLatency=") {
		url = addQuery(url, "lowLatency", "true")
	}

	return url, nil
}

func getHostname() string {
	execution := os.Getenv("CLOUD_RUN_EXECUTION")
	taskIndex := os.Getenv("CLOUD_RUN_TASK_INDEX")

	if execution != "" && taskIndex != "" {
		// extract only the final hash on the execution name
		parts := strings.Split(execution, "-")
		hash := parts[len(parts)-1]
		return fmt.Sprintf("exec-%s-task-%s", hash, taskIndex)
	}

	hostname, _ := os.Hostname()
	return hostname
}

func getRegion() string {
	if zone := os.Getenv("ZONE"); zone != "" {
		return zone
	} else if gceZone := os.Getenv("GCE_ZONE"); gceZone != "" {
		return gceZone
	}
	return ""
}

func addQuery(urlStr, name, value string) string {
	u, err := url.Parse(urlStr)
	if err != nil {
		panic(fmt.Errorf("invalid URL (%s): %w", urlStr, err))
	}

	query := u.Query()
	query.Set(name, value)
	u.RawQuery = query.Encode()

	return u.String()
}

func signJwt(playbackID, privateKey string, ttl time.Duration) (string, error) {
	decodedPrivateKey, _ := base64.StdEncoding.DecodeString(privateKey)
	bytesPrivateKey := []byte(decodedPrivateKey)

	pk, err := jwt.ParseECPrivateKeyFromPEM(bytesPrivateKey)
	if err != nil {
		return "", err
	}

	publicKey, err := derivePublicKey(privateKey)
	if err != nil {
		return "", err
	}

	unsignedToken := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"sub": playbackID,
		"pub": publicKey,
		"exp": time.Now().Add(ttl).Unix(),
	})

	token, err := unsignedToken.SignedString(pk)
	if err != nil {
		return "", err
	}

	return token, nil
}

func derivePublicKey(privateKey string) (string, error) {
	decodedPrivateKey, err := base64.StdEncoding.DecodeString(privateKey)
	if err != nil {
		return "", err
	}

	pk, err := jwt.ParseECPrivateKeyFromPEM([]byte(decodedPrivateKey))
	if err != nil {
		return "", err
	}

	pubKeyPKIX, err := x509.MarshalPKIXPublicKey(&pk.PublicKey)
	if err != nil {
		return "", err
	}

	pubKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubKeyPKIX,
	})

	return base64.StdEncoding.EncodeToString(pubKeyPEM), nil
}
