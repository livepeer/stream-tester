FROM livepeerci/build AS builder

RUN apt install -y wget tar

WORKDIR /root
RUN wget https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs.mp4
RUN wget https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs_3min.mp4
RUN wget https://storage.googleapis.com/lp_testharness_assets/bbb_sunflower_1080p_30fps_normal_t02.mp4
RUN wget https://storage.googleapis.com/lp_testharness_assets/bbb_sunflower_1080p_30fps_normal_2min.mp4
RUN wget https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs_30s.mp4
RUN wget -qO- https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs_30s_hls.tar.gz | tar xvz -C .

COPY go.mod go.mod
COPY go.sum go.sum

RUN go mod download

ARG version
RUN echo $version

COPY . .

RUN go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" -tags h264 cmd/streamtester/streamtester.go
RUN go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" cmd/testdriver/testdriver.go
RUN go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" cmd/mist-api-connector/mist-api-connector.go
RUN go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" cmd/loadtester/loadtester.go
RUN go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" cmd/stream-monitor/stream-monitor.go
RUN go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" cmd/orch-tester/orch_tester.go
RUN go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" cmd/recordtester/recordtester.go

FROM debian:stretch-slim

RUN apt update \
  && apt install -y ffmpeg ca-certificates \
  && apt clean && apt autoclean
RUN ffmpeg -version
RUN update-ca-certificates

WORKDIR /root
ENV PKG_CONFIG_PATH /root/compiled/lib/pkgconfig

COPY --from=builder /root/compiled /root/compiled/

COPY --from=builder /root/official_test_source_2s_keys_24pfs.mp4 official_test_source_2s_keys_24pfs.mp4
COPY --from=builder /root/official_test_source_2s_keys_24pfs_3min.mp4 official_test_source_2s_keys_24pfs_3min.mp4
COPY --from=builder /root/bbb_sunflower_1080p_30fps_normal_t02.mp4 bbb_sunflower_1080p_30fps_normal_t02.mp4
COPY --from=builder /root/bbb_sunflower_1080p_30fps_normal_2min.mp4 bbb_sunflower_1080p_30fps_normal_2min.mp4
COPY --from=builder /root/official_test_source_2s_keys_24pfs_30s.mp4 official_test_source_2s_keys_24pfs_30s.mp4
COPY --from=builder /root/official_test_source_2s_keys_24pfs_30s_hls official_test_source_2s_keys_24pfs_30s_hls

COPY --from=builder /root/streamtester streamtester
COPY --from=builder /root/testdriver testdriver
COPY --from=builder /root/mist-api-connector mist-api-connector
COPY --from=builder /root/loadtester loadtester
COPY --from=builder /root/stream-monitor stream-monitor
COPY --from=builder /root/orch_tester orch_tester
COPY --from=builder /root/recordtester recordtester
