# Stream tester

Stream tester is a tool to test Livepeer network performance and stability.
It sends RTMP streams into broadcaster nodes and download back transcoded data and calculates success rate.
It can be used as standalone command line tool or as part of [test harness](https://github.com/livepeer/test-harness).
As part of test harness, it works in server mode, inside docker image, and is controlled through REST interface.


## Installation

Dependencies:
`go get -u -v github.com/gosuri/uiprogress`
`go get -u -v github.com/ericxtang/m3u8`
`go get -u -v github.com/golang/glog`
`go get -u -v github.com/nareix/joy4`


## Command line
Usage:
`./streamtester -host localhost -rtmp 1935 -media 8935 -profiles 2 -repeat 1 -sim 1 file_to_stream.mp4`


Params:

 - `-host` Host name of broadcaster to stream to
 - `-rtmp` Port number to stream RTMP stream to
 - `-media` Port number to download media from
 - `-profiles` How many transcoding profiles broadcaster configured with
 - `-sim` How many simultaneous streams stream into broadcaster
 - `-repeat` How many times to repeat streaming 


## Server mode
Run
`./streamtester -server -serverAddr  localhost:7934`

This command block until interrupted.

It provide these REST endpoints:

---
GET `/stats`

returns object:
```json
{
  "rtmp_active_streams": 1,
  "rtm_pstreams": 1,
  "media_streams": 1,
  "sent_segments": 3,
  "downloaded_segments": 7,
  "should_have_downloaded_segments": 9,
  "failed_to_download_segments": 0,
  "bytes_downloaded": 2721864,
  "sucess_rate": 77.77777777777779,
  "connection_lost": 0,
  "finished": false
}
```

Where 

 - `rtmp_active_streams` - number of streams is being currently streamed
 - `rtm_pstreams` - total number of RTMP streams (active plus finished)
 - `rtm_pstreams` - total number of media streams (downloading back segments)
 - `sent_segments` - total number of segments sent to broadcaster
 - `downloaded_segments` - total number of segments downloaded back
 - `should_have_downloaded_segments` - number of segments should be downloaded back for 100% success rate. It equals `sent_segments` * (`numberOfTranscodingProfiles` + 1)
 - `failed_to_download_segments` - number of segments failed to download with some error
 - `bytes_downloaded` - number of bytes downloaded
 - `sucess_rate` - success rate, percent
 - `connection_lost` - number of times streamer lost connection to broadcaster. Streamer does not attempt to reconnect, so statistics will only have data till connection was lost
 - `finished` - indicates that all streaming tasks are finished

---
POST `/start_streams`

Accepts object:
```json
{
    "host": "localhost",
    "file_name": "official_test_source_2s_keys.mp4",
    "rtmp": 1935,
    "media": 8935,
    "repeat": 1,
    "simultaneous": 1,
    "profiles_num": 2,
} 

```

`file_name` - should exists in local filesystem of streamer.


Returns 

```json
{"success": true}
```

Can return 404 if request contains wrong parameters.

