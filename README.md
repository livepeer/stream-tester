# Stream tester

Stream tester is a tool to test Livepeer network performance and stability.
It sends RTMP streams into broadcaster nodes and download back transcoded data and calculates success rate.
It can be used as standalone command line tool or as part of [test harness](https://github.com/livepeer/test-harness).
As part of test harness, it works in server mode, inside docker image, and is controlled through REST interface.

Also it can be used to pull arbitrary .m3u8 stream and save it to disk.

When running from command line, Stream tester has two different modes.
First one is for (load) testing Livepeer Broadcaster and second one was developed for running long test against Livepeer Wowza plugin, but can be used with any transcoder that takes RTMP input and output HLS.


## Binaries
Automatically built binaries can be found on [releases](https://github.com/livepeer/stream-tester/releases) page.



## Command line

If name of the file to stream is not specified, then default one - `official_test_source_2s_keys_24pfs.mp4` is used. It should be placed in the same directory as Stream Tester. Can be downloaded from [here](https://storage.googleapis.com/lp_testharness_assets/official_test_source_2s_keys_24pfs.mp4).

All options can be put into config file and used like `./streamtester -config local.cfg`

### Load testing mode

Streams file to its end, or for the time specified. Can stream arbitrary number of streams simultaneously and repeat streaming any number of times. Counts number of segments streamed and number of segments that was readed back. Reports success rate (readed segments / segments should have been readed). Calculate transcode latency (should be enabled from command line).

Usage:
`./streamtester -host localhost -rtmp 1935 -media 8935 -profiles 2 -repeat 1 -sim 1 file_to_stream.mp4`


Params:

 - `-host` Host name of broadcaster to stream to
 - `-media-host` Host name to read transcoded stream from. If not specified, `-host` will be used
 - `-rtmp` Port number to stream RTMP stream to
 - `-media` Port number to download media from
 - `-profiles` How many transcoding profiles broadcaster configured with
 - `-sim` How many simultaneous streams stream into broadcaster
 - `-repeat` How many times to repeat streaming 
 - `-latency` Measure transcoding latency
 - `-time` Time to stream streams (40s, 4m, 24h45m). Not compatible with repeat option
 - `-http-ingest` Use HTTP push instead of RTMP

### Infinite stream testing mode
In this mode Stream Tester streams video to RTMP ingest point and read HLS stream back. Streaming is stopped only on error, so it will be infinite if transcoding is done ideally.

Errors can be reported to Discord.

Checks downloaded segments for validity by parsing them using `joy4` lib. Can save segments with errors to Google Storage.

Checks for gaps in HLS stream (PTS of the first frame of the segment should be PTS of first frame of previous segment plus length of previous segment).

Checks for time drift between transcoded streams (At each given time, PTSs of first segments in different media streams shouldn't differ for more than four seconds. (Apple's HLS validator tool report error if difference is more than two seconds)).

Stops streaming if there is no new segments in HLS stream for a 30 seconds.


Usage:
`./streamtester -ignore-time-drift -ignore-gaps -wowza -wait-for-target 150s -media-url http://site.com:1935/something.m3u8 -rtmp-url rtmp://site.com:1935/something -profiles 3   file_to_stream.mp4`


Params:

 - `-wowza` Should be specified if streaming to Wowza. Removes Wowza's session cookies from manifest names
 - `-rtmp-url` URL to stream RTMP stream to
 - `-media-url` URL of main .m3u8 manifest to pull transcoded stream back from
 - `-profiles` How many transcoded (not including source) profiles should be in resulting (.m3u8) stream
 - `ignore-no-codec-error` Do not stop streaming if segment without codec's info downloaded
 - `ignore-gaps` Do not stop streaming if gaps found
 - `ignore-time-drift` Do not stop streaming if time drift detected
 - `wait-for-target` Timeout for trying to connect to RTMP ingest host. Should be specified in seconds or minutes (10s, 4m)
 - `discord-url` URL of Discord's webhook to send messages to Discord channel
 - `discord-users` Id's of users to notify in case of failure
 - `discord-user-name` User name to use when sending messages to Discord
 - `gsbucket` Google Storage bucket (to store segments that was not successfully parsed)
 - `gskey` Google Storage private key (in json format (actual key, not file name))

### Infinite HLS pull testing mode
In this mode Stream Tester pulls arbitrary HLS stream and runs same checks as in previous mode.
To use it specify `-media-url` without specifying `-rtmp-url`.




### Saving arbitrary stream to file

Running

`./streamtester -save -infinite-pull http://site.com:1935/live/main_playlist.m3u8`

will save pull `main_playlist.m3u8` stream and save all the segments along with (`VOD`) manifests to current directory.

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
  "success_rate": 77.77777777777779,
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
 - `success_rate` - success rate, percent
 - `connection_lost` - number of times streamer lost connection to broadcaster. Streamer does not attempt to reconnect, so statistics will only have data till connection was lost
 - `finished` - indicates that all streaming tasks are finished

---
POST `/start_streams`

Accepts object:
```json
{
    "host": "localhost",
    "media_host": "",
    "file_name": "official_test_source_2s_keys_24pfs.mp4",
    "rtmp": 1935,
    "media": 8935,
    "repeat": 1,
    "simultaneous": 1,
    "profiles_num": 2,
    "do_not_clear_stats": false,
    "http_ingest": false
} 

```

`file_name` - should exists in local filesystem of streamer.
`do_not_clear_stats` - if true, on new call to `/start_streams` do not clear old stats, but instead append to it


Returns 

```json
{"success": true}
```

Can return 404 if request contains wrong parameters.


---
GET `/stop`

Stop currently running streams. 

Doesn't return any data, only status 200 
