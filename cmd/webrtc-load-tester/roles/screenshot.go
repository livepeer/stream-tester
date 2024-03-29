// Command screenshot is a chromedp example demonstrating how to take a
// screenshot of a specific element and of the entire browser viewport.
package roles

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/chromedp/chromedp"
)

func Screenshot() {
	// create context
	ctx, cancel := chromedp.NewContext(
		context.Background(),
		// chromedp.WithDebugf(log.Printf),
		chromedp.WithBrowserOption(chromedp.WithBrowserLogf(log.Printf)),
	)
	defer cancel()

	// capture screenshot of an element
	var buf []byte
	if err := chromedp.Run(ctx, elementScreenshot(`https://pkg.go.dev/`, `img.Homepage-logo`, &buf)); err != nil {
		log.Fatal(err)
	}

	if err := os.WriteFile("elementScreenshot.png", buf, 0o644); err != nil {
		log.Fatal(err)
	}

	// capture entire browser viewport, returning png with quality=90
	tasks := chromedp.Tasks{
		chromedp.Navigate(`https://lvpr.tv/?v=be56lkusq0flc869&lowLatency=force`),
		chromedp.Sleep(5 * time.Minute),
		chromedp.FullScreenshot(&buf, 90),
	}
	if err := chromedp.Run(ctx, tasks); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile("fullScreenshot.png", buf, 0o644); err != nil {
		log.Fatal(err)
	}

	log.Printf("wrote elementScreenshot.png and fullScreenshot.png")
}

// elementScreenshot takes a screenshot of a specific element.
func elementScreenshot(urlstr, sel string, res *[]byte) chromedp.Tasks {
	return chromedp.Tasks{
		chromedp.Navigate(urlstr),
		chromedp.Screenshot(sel, res, chromedp.NodeVisible),
	}
}
