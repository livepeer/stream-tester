module github.com/livepeer/stream-tester

go 1.17

require (
	cloud.google.com/go/storage v1.15.0
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Necroforger/dgrouter v0.0.0-20190528143456-040421b5a83e
	github.com/PagerDuty/go-pagerduty v1.3.0
	github.com/bwmarrin/discordgo v0.20.2
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529
	github.com/gosuri/uiprogress v0.0.1
	github.com/livepeer/joy4 v0.1.2-0.20220210094601-95e4d28f5f07
	github.com/livepeer/leaderboard-serverless v1.0.0
	github.com/livepeer/livepeer-data v0.4.0
	github.com/livepeer/m3u8 v0.11.1
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/peterbourgon/ff v1.7.0
	github.com/peterbourgon/ff/v2 v2.0.0
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.15.0
	github.com/rabbitmq/amqp091-go v1.1.0
	github.com/spf13/cobra v1.2.1
	go.etcd.io/etcd/client/pkg/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0-rc.0
	go.opencensus.io v0.23.0
	golang.org/x/net v0.0.0-20210503060351-7fd8e65b6420
	golang.org/x/text v0.3.6
	google.golang.org/api v0.46.0
	google.golang.org/grpc v1.38.0
)

require (
	cloud.google.com/go v0.81.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.1 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/gosuri/uilive v0.0.3 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/mattn/go-ieproxy v0.0.0-20190610004146-91bb50d98149 // indirect
	github.com/mattn/go-isatty v0.0.8 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/rabbitmq/rabbitmq-stream-go-client v0.1.0-beta.0.20211027081212-fd5e6d497413 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.etcd.io/etcd/api/v3 v3.5.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/crypto v0.0.0-20201002170205-7f63de1d35b0 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.4.2 // indirect
	golang.org/x/oauth2 v0.0.0-20210427180440-81ed05c6b58c // indirect
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007 // indirect
	golang.org/x/tools v0.1.2 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	google.golang.org/protobuf v1.26.0 // indirect
)

exclude github.com/gosuri/uilive v0.0.4 // cause memory corruption

// replace github.com/livepeer/joy4 => /Users/dark/projects/livepeer/joy4

replace github.com/ethereum/go-ethereum => github.com/ethereum/go-ethereum v1.9.3
