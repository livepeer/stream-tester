package testers

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/broadcaster"
)

// SaveNewStreams monitors /status endpoint of provided broadcasters, and then 1 new stream
// arrives - pulls it and save to files
func SaveNewStreams(ctx context.Context, broadcasters ...string) {

	if len(broadcasters) == 0 {
		panic("no broadcasters provided")
	}
	go saveNewStreams(ctx, broadcasters)
}

func saveNewStreams(ctx context.Context, broadcasters []string) {
	initialStatuses := make([]*broadcaster.StatusResp, 0, len(broadcasters))
	uris := make([]string, 0, len(broadcasters))
	for _, bs := range broadcasters {
		uri := fmt.Sprintf("http://%s:7935/status", bs)
		uris = append(uris, uri)
		st, err := broadcaster.Status(uri)
		if err != nil {
			glog.Fatal("Can't get status from broadcaster", uri, err)
		}
		initialStatuses = append(initialStatuses, st)
	}
	var bhost, streamToSave string
	// waiting for new stream
stopwaiting:
	for {
		time.Sleep(time.Second)
		select {
		case <-ctx.Done():
			return
		default:
		}
		for i, uri := range uris {
			st, err := broadcaster.Status(uri)
			if err != nil {
				glog.Error("Can't get status from broadcaster", uri, err)
				continue
			}
			newStreams := st.NewerManifests(initialStatuses[i])
			if len(newStreams) > 0 {
				glog.Infof("Got new manifests: %+v", newStreams)
				streamToSave = newStreams[0]
				bhost = broadcasters[i]
				break stopwaiting
			}
		}
	}
	// start stream save
	mt := newM3UTester(ctx, nil, false, false, false, true, true, nil, nil, "")
	su := fmt.Sprintf("http://%s:8935/stream/%s.m3u8", bhost, streamToSave)
	glog.Infof("Start saving to disk %s", su)
	mt.Start(su)
}
