module github.com/livepeer/stream-tester

go 1.13

// TODO: Revert before merging. Won't build in the CI anyway
replace github.com/livepeer/livepeer-data => ../livepeer-data

require (
	cloud.google.com/go/storage v1.15.0
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/go-autorest/autorest/adal v0.9.5 // indirect
	github.com/Necroforger/dgrouter v0.0.0-20190528143456-040421b5a83e
	github.com/PagerDuty/go-pagerduty v1.3.0
	github.com/bwmarrin/discordgo v0.20.2
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/gosuri/uilive v0.0.3 // indirect
	github.com/gosuri/uiprogress v0.0.1
	github.com/livepeer/joy4 v0.1.2-0.20210601043311-c1b885884cc7
	github.com/livepeer/leaderboard-serverless v1.0.0
	github.com/livepeer/livepeer-data v0.4.0
	github.com/livepeer/m3u8 v0.11.1
	github.com/mattn/go-isatty v0.0.8 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/peterbourgon/ff v1.7.0
	github.com/peterbourgon/ff/v2 v2.0.0
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.15.0
	github.com/rabbitmq/amqp091-go v1.1.0
	go.etcd.io/etcd/client/pkg/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0-rc.0
	go.opencensus.io v0.23.0
	golang.org/x/net v0.0.0-20210503060351-7fd8e65b6420
	golang.org/x/text v0.3.6
	google.golang.org/api v0.46.0
	google.golang.org/grpc v1.38.0
)

exclude github.com/gosuri/uilive v0.0.4 // cause memory corruption

// replace github.com/livepeer/joy4 => /Users/dark/projects/livepeer/joy4

replace github.com/ethereum/go-ethereum => github.com/ethereum/go-ethereum v1.9.3
