package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/livepeer/joy4/av"
	"github.com/livepeer/joy4/av/avutil"
	"github.com/livepeer/joy4/format"
	"github.com/livepeer/joy4/format/ts"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/spf13/cobra"
)

const (
	appName = "api-transcoder"
	segLen  = 18 * time.Second
)

func init() {
	format.RegisterAll()
	rand.Seed(time.Now().UnixNano())
}

var errResolution = errors.New("InvalidResolution")
var errH264Profile = errors.New("InvalidH264Profile")

var allowedExt = []string{".ts", ".mp4"}
var allowedH264Profiles = map[string]string{"baseline": "H264Baseline", "main": "H264Main", "high": "H264High"}

/*
   - H264Baseline
   - H264Main
   - H264High
   - H264ConstrainedHigh
*/

func parseResolution(resolution string) (int, int, error) {
	res := strings.Split(resolution, "x")
	if len(res) < 2 {
		return 0, 0, errResolution
	}
	w, err := strconv.Atoi(res[0])
	if err != nil {
		return 0, 0, err
	}
	h, err := strconv.Atoi(res[1])
	if err != nil {
		return 0, 0, err
	}
	return w, h, nil
}

func parseFps(fps string) (int, int, error) {
	if len(fps) == 0 {
		return 0, 0, nil
	}
	fpp := strings.Split(fps, "/")
	var den uint64
	num, err := strconv.ParseUint(fpp[0], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing fps %w", err)
	}
	if len(fpp) > 1 {
		den, err = strconv.ParseUint(fpp[1], 10, 32)
		if err != nil {
			return 0, 0, fmt.Errorf("error parsing fps %w", err)
		}
	}
	return int(num), int(den), nil
}

func parmsToProfile(resolution, h264profile, frameRate string, bitrate uint64, gop time.Duration) (*livepeer.Profile, error) {
	var res = &livepeer.Profile{
		Name:    "custom",
		Bitrate: int(bitrate) * 1000,
	}
	if bitrate == 0 {
		return nil, fmt.Errorf("should also specify bitrate")
	}
	if gop > 0 {
		res.Gop = strconv.FormatFloat(gop.Seconds(), 'f', 4, 64)
	}
	w, h, err := parseResolution(resolution)
	if err != nil {
		return nil, err
	}
	res.Width = w
	res.Height = h
	num, den, err := parseFps(frameRate)
	if err != nil {
		return nil, err
	}
	res.Fps = num
	res.FpsDen = den
	if len(h264profile) > 0 {
		if hp, ok := allowedH264Profiles[h264profile]; !ok {
			return nil, errH264Profile
		} else {
			res.Profile = hp
		}
	}

	return res, nil
}

func makeDstName(dst string, i, profiles int) string {
	if profiles == 1 {
		return dst
	}
	ext := filepath.Ext(dst)
	base := strings.TrimSuffix(dst, ext)
	return fmt.Sprintf("%s_%d%s", base, i, ext)
}

func transcode(apiKey, apiHost, src, dst string, presets []string, lprofile *livepeer.Profile) error {
	lapi := livepeer.NewLivepeer2(apiKey, apiHost, nil, 2*time.Minute)
	lapi.Init()
	fmt.Printf("Choosen API server: %s", lapi.GetServer())
	streamName := fmt.Sprintf("tod_%s", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	// stream, err := lapi.CreateStreamEx(streamName, true, nil, standardProfiles...)
	// presets := []string{"P144p30fps16x9", "P240p30fps4x3"}
	var profiles []livepeer.Profile
	if lprofile != nil {
		profiles = append(profiles, *lprofile)
	}
	stream, err := lapi.CreateStreamEx(streamName, false, presets, profiles...)
	if err != nil {
		return err
	}

	defer func(sid string) {
		lapi.DeleteStream(sid)
	}(stream.ID)
	fmt.Printf("Created stream id=%s name=%s\n", stream.ID, stream.Name)
	gctx, gcancel := context.WithCancel(context.TODO())
	defer gcancel()
	segmentsIn := make(chan *model.HlsSegment)
	if err = testers.StartSegmenting(gctx, src, true, 0, 0, segLen, false, segmentsIn); err != nil {
		return err
	}
	var outFiles []av.MuxCloser
	var dstNames []string
	if len(presets) == 0 {
		presets = append(presets, "dummy")
	}
	for i := range presets {
		// dstName := fmt.Sprintf(dstNameTemplate, i)
		dstName := makeDstName(dst, i, len(presets))
		dstNames = append(dstNames, dstName)
		dstFile, err := avutil.Create(dstName)
		if err != nil {
			return fmt.Errorf("can't create out file %w", err)
		}
		outFiles = append(outFiles, dstFile)
	}

	var transcoded [][]byte
	for seg := range segmentsIn {
		if seg.Err == io.EOF {
			break
		}
		if seg.Err != nil {
			err = seg.Err
			break
		}
		fmt.Printf("Got segment seqNo=%d pts=%s dur=%s data len bytes=%d\n", seg.SeqNo, seg.Pts, seg.Duration, len(seg.Data))
		started := time.Now()
		transcoded, err = lapi.PushSegment(stream.ID, seg.SeqNo, seg.Duration, seg.Data)
		if err != nil {
			break
		}
		fmt.Printf("Transcoded %d took %s\n", len(transcoded), time.Since(started))

		for i, segData := range transcoded {
			demuxer := ts.NewDemuxer(bytes.NewReader(segData))
			if seg.SeqNo == 0 {
				streams, err := demuxer.Streams()
				if err != nil {
					return err
				}
				if err = outFiles[i].WriteHeader(streams); err != nil {
					return err
				}
			}
			if err = avutil.CopyPackets(outFiles[i], demuxer); err != io.EOF {
				return err
			}
		}
	}
	for _, outFile := range outFiles {
		if err = outFile.Close(); err != nil {
			return err
		}
	}
	gcancel()
	if err != nil {
		fmt.Printf("Error while segmenting err=%v\n", err)
	}
	fmt.Printf("Written files:\n")
	for i := range outFiles {
		// dstName := fmt.Sprintf(dstNameTemplate, i)
		fmt.Printf("    %s\n", dstNames[i])
	}
	return nil
}

func main() {
	flag.Set("logtostderr", "true")
	// flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	// flag.Parse()
	flag.CommandLine.Parse(nil)
	/*
		vFlag := flag.Lookup("v")
		verbosity := "6"

		flag.CommandLine.Parse(nil)
		vFlag.Value.Set(verbosity)
	*/
	// var echoTimes int
	var apiKey, apiHost string
	var presets string
	var resolution, frameRate, profile string
	var bitrateK uint64
	var gop time.Duration

	var cmdTranscode = &cobra.Command{
		Use:   "transcode input.[ts|mp4] output.[ts|mp4]",
		Short: "Transcodes video file using Livepeer API",
		Long:  `Transcodes video file using Livepeer API.`,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			// fmt.Println("transcode: " + strings.Join(args, " "))
			var err error
			inp := args[0]
			inpExt := filepath.Ext(inp)
			if !utils.StringsSliceContains(allowedExt, inpExt) {
				fmt.Fprintf(os.Stderr, "Unsupported extension %q for file %q\n", inpExt, inp)
				os.Exit(1)
			}
			output := args[1]
			outputExt := filepath.Ext(output)
			if !utils.StringsSliceContains(allowedExt, outputExt) {
				fmt.Fprintf(os.Stderr, "Unsupported extension %q for file %q\n", outputExt, output)
				os.Exit(1)
			}
			if apiKey == "" {
				fmt.Printf("Should provide --api-key flag\n")
				os.Exit(1)
			}
			if stat, err := os.Stat(inp); errors.Is(err, fs.ErrNotExist) {
				fmt.Fprintf(os.Stderr, "File %q does not exists\n", inp)
				os.Exit(1)
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "For file: %q err %v\n", inp, err)
				os.Exit(1)
			} else if stat.IsDir() {
				fmt.Fprintf(os.Stderr, "Not a file: %q\n", inp)
				os.Exit(1)
			}
			fmt.Printf("api key %q transcode from %q to %q\n", apiKey, inp, output)
			// presets := []string{"P144p30fps16x9", "P240p30fps4x3"}
			// ffmpeg.P720p30fps16x9
			var presetsAr []string
			if len(presets) > 0 {
				presetsAr = strings.Split(presets, ",")
				fmt.Printf("presets %q ar %+v\n", presets, presetsAr)
				if len(presetsAr) > 0 {
					for _, pr := range presetsAr {
						if _, ok := mist.ProfileLookup[pr]; !ok {
							fmt.Printf("Unknown preset name: %q\n", pr)
							os.Exit(1)
						}
					}
				}
			}

			if len(presets) == 0 && len(resolution) == 0 {
				fmt.Printf("Should specify preset or resolution\n")
				os.Exit(1)
			}
			var transcodeProfile *livepeer.Profile
			if len(resolution) > 0 {
				if transcodeProfile, err = parmsToProfile(resolution, profile, frameRate, bitrateK, gop); err != nil {
					fmt.Printf("Error parsing arguments: %s\n", err)
					os.Exit(1)
				}

			}
			if err := transcode(apiKey, apiHost, inp, output, presetsAr, transcodeProfile); err != nil {
				fmt.Fprintf(os.Stderr, "Error while transcoding: %v\n", err)
				os.Exit(2)
			}
		},
	}
	cmdTranscode.Flags().StringVarP(&presets, "presets", "p", "", "List of transcoding presets, comma separated (P720p30fps16x9, etc)")
	cmdTranscode.Flags().StringVarP(&frameRate, "framerate", "f", "", "Frame rate")
	cmdTranscode.Flags().StringVarP(&resolution, "resolution", "r", "", "Resolution (1280x720)")
	cmdTranscode.Flags().StringVarP(&profile, "profile", "o", "", "Profile (baseline,main,high)")
	cmdTranscode.Flags().Uint64VarP(&bitrateK, "bitrate", "b", 0, "Bitrate (in Kbit)")
	cmdTranscode.Flags().DurationVarP(&gop, "gop", "g", 0, "Gop (time between keyframes)")

	var cmdListPresets = &cobra.Command{
		Use:   "list-presets ",
		Short: "Lists transcoding presets",
		Long:  `Lists available transcoding presets`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Available transcoding presets:\n")
			var ps []string
			for k := range mist.ProfileLookup {
				ps = append(ps, k)
			}
			sort.Strings(ps)
			for _, pr := range ps {
				fmt.Printf("  %s\n", pr)
			}
		},
	}

	var rootCmd = &cobra.Command{
		Use:               appName,
		Version:           model.Version,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	rootCmd.PersistentFlags().StringVarP(&apiKey, "api-key", "k", "", "Livepeer API key")
	rootCmd.PersistentFlags().StringVarP(&apiHost, "api-host", "a", "livepeer.com", "Livepeer API host")
	rootCmd.MarkFlagRequired("api-key")
	rootCmd.AddCommand(cmdTranscode)
	rootCmd.AddCommand(cmdListPresets)
	rootCmd.Execute()
}
