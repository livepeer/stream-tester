module github.com/livepeer/stream-tester

go 1.13

require (
	cloud.google.com/go/storage v1.4.0
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/go-autorest/autorest/adal v0.9.5 // indirect
	github.com/Necroforger/dgrouter v0.0.0-20190528143456-040421b5a83e
	github.com/PagerDuty/go-pagerduty v1.3.0 // indirect
	github.com/bwmarrin/discordgo v0.20.2
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/gosuri/uilive v0.0.3 // indirect
	github.com/gosuri/uiprogress v0.0.1
	// github.com/livepeer/joy4 v0.1.2-0.20191220171501-3d0cd11ebf39
	github.com/livepeer/joy4 v0.1.2-0.20210211183238-dc472660ed3b
	github.com/livepeer/leaderboard-serverless v1.0.0
	github.com/livepeer/m3u8 v0.11.0
	github.com/mattn/go-isatty v0.0.8 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/peterbourgon/ff v1.6.0
	github.com/peterbourgon/ff/v2 v2.0.0
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.15.0
	go.opencensus.io v0.22.2
	golang.org/x/lint v0.0.0-20200130185559-910be7a94367 // indirect
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	golang.org/x/text v0.3.3
	golang.org/x/tools v0.0.0-20200204192400-7124308813f3 // indirect
	google.golang.org/api v0.14.0
)

exclude github.com/gosuri/uilive v0.0.4 // cause memory corruption

// replace github.com/livepeer/joy4 => /Users/dark/projects/livepeer/joy4

replace github.com/ethereum/go-ethereum => github.com/ethereum/go-ethereum v1.9.3
