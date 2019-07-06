FROM golang:1-alpine as builder

RUN apk add --no-cache git

WORKDIR /root
RUN wget https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs.mp4
RUN wget https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs_3min.mp4

COPY cmd cmd 
COPY internal internal
COPY go.mod go.mod
COPY go.sum go.sum

RUN go build cmd/streamtester/streamtester.go


FROM alpine
RUN apk add --no-cache ca-certificates

WORKDIR /root
COPY --from=builder /root/streamtester streamtester
COPY --from=builder /root/official_test_source_2s_keys_24pfs.mp4 official_test_source_2s_keys_24pfs.mp4
COPY --from=builder /root/official_test_source_2s_keys_24pfs_3min.mp4 official_test_source_2s_keys_24pfs_3min.mp4

# docker build -t livepeer/streamtester:latest .
# docker push livepeer/streamtester:latest
