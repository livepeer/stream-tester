FROM golang:1-alpine as builder

RUN apk add --no-cache git

WORKDIR /go/src/github.com/livepeer/stream-tester
RUN wget https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs.mp4
RUN wget https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs_3min.mp4

RUN go get -u -v github.com/gosuri/uiprogress
RUN go get -u -v github.com/golang/glog
RUN go get -u -v github.com/nareix/joy4
RUN go get -u -v golang.org/x/text/message
RUN go get -u -v github.com/ericxtang/m3u8

COPY cmd cmd 
COPY internal internal

RUN go build cmd/streamtester/streamtester.go


FROM alpine

WORKDIR /root
COPY --from=builder /go/src/github.com/livepeer/stream-tester/streamtester streamtester
COPY --from=builder /go/src/github.com/livepeer/stream-tester/official_test_source_2s_keys_24pfs.mp4 official_test_source_2s_keys_24pfs.mp4
COPY --from=builder /go/src/github.com/livepeer/stream-tester/official_test_source_2s_keys_24pfs_3min.mp4 official_test_source_2s_keys_24pfs_3min.mp4

# docker build -t livepeer/streamtester:latest .
