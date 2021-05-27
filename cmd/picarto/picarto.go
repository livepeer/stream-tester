package main

import (
	"flag"
	"fmt"

	"github.com/livepeer/stream-tester/apis/picarto"
)

func main() {
	flag.Set("logtostderr", "true")
	// vFlag := flag.Lookup("v")
	fmt.Printf("Started")
	// picartoCountry := "us-east1"
	picartoCountry := ""
	ps, err := picarto.GetOnlineUsers(picartoCountry, true, false)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got %d records\n", len(ps))
	for _, pr := range ps {
		if pr.Adult {
			// fmt.Printf("%s %v %s %s\n", pr.Name, pr.Adult, pr.Title, pr.Thumbnails["web"])
			fmt.Printf("https://picarto.tv/%s  %s\n", pr.Name, pr.Thumbnails["web"])
		}
	}
}
