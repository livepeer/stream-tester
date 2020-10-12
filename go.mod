module github.com/livepeer/stream-tester

go 1.13

require (
	cloud.google.com/go/storage v1.4.0
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Necroforger/dgrouter v0.0.0-20190528143456-040421b5a83e
	github.com/bwmarrin/discordgo v0.20.2
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/gosuri/uilive v0.0.3 // indirect
	github.com/gosuri/uiprogress v0.0.1
	// github.com/livepeer/joy4 v0.1.2-0.20191220171501-3d0cd11ebf39
	github.com/livepeer/joy4 v0.1.2-0.20201012094723-a5f514865714
	github.com/livepeer/m3u8 v0.11.0
	github.com/mattn/go-isatty v0.0.8 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/peterbourgon/ff v1.6.0
	github.com/peterbourgon/ff/v2 v2.0.0
	github.com/prometheus/client_golang v1.5.1
	go.opencensus.io v0.22.0
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
	golang.org/x/text v0.3.2
	google.golang.org/api v0.14.0
)

exclude github.com/gosuri/uilive v0.0.4 // cause memory corruption

// replace github.com/livepeer/joy4 => /Users/dark/projects/livepeer/joy4
