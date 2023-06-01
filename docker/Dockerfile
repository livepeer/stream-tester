FROM	golang:1.19-alpine3.15	as	builder

RUN	apk add --no-cache make gcc musl-dev linux-headers git \
	pkgconfig gnutls-dev ffmpeg-dev build-base parallel

WORKDIR	/root

RUN	parallel wget https://storage.googleapis.com/lp_testharness_assets/{} ::: official_test_source_2s_keys_24pfs.mp4 official_test_source_2s_keys_24pfs_3min.mp4 bbb_sunflower_1080p_30fps_normal_t02.mp4 bbb_sunflower_1080p_30fps_normal_2min.mp4 official_test_source_2s_keys_24pfs_30s.mp4 && \
	wget -qO- https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs_30s_hls.tar.gz | tar xvz -C .

COPY	go.mod	go.sum	./

RUN	go mod download

COPY	.	.

ARG	version

RUN	go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" -tags h264 cmd/streamtester/streamtester.go

RUN	parallel -q go build -ldflags="-X 'github.com/livepeer/stream-tester/model.Version=$version' -X 'github.com/livepeer/stream-tester/model.IProduction=true'" cmd/{}/{}.go ::: testdriver loadtester stream-monitor recordtester

FROM	alpine:3.17.3

RUN	apk add --no-cache ca-certificates ffmpeg

WORKDIR	/root

COPY --from=builder	/root/*.mp4	./
COPY --from=builder	/root/official_test_source_2s_keys_24pfs_30s_hls	official_test_source_2s_keys_24pfs_30s_hls

COPY --from=builder	/root/streamtester	\
	/root/testdriver	\
	/root/loadtester	\
	/root/stream-monitor	\
	/root/recordtester	/usr/local/bin/

RUN	for b in testdriver loadtester stream-monitor recordtester; do ln -s /usr/local/bin/$b ./$b; done
