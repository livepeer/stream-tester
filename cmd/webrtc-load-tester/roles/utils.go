package roles

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2"
)

func parseFlags(registerVars func(*flag.FlagSet)) {
	flag.Set("logtostderr", "true")

	fs := flag.NewFlagSet("webrtc-load-tester", flag.ExitOnError)

	registerVars(fs)

	_ = fs.String("config", "", "config file (optional)")

	role := os.Args[1]
	err := ff.Parse(fs, os.Args[2:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("LT_WEBRTC"),
	)
	if err != nil {
		glog.Fatal("Error parsing args: ", err)
	}

	err = flag.CommandLine.Parse(nil)
	if err != nil {
		glog.Fatal("Error parsing args: ", err)
	}

	hostName, _ := os.Hostname()
	fmt.Printf("WebRTC Load Tester (%s) version: %s\n", role, model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)
}

func signalContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		waitSignal(syscall.SIGINT, syscall.SIGTERM)
	}()
	return ctx
}

func waitSignal(sigs ...os.Signal) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, sigs...)
	defer signal.Stop(sigc)

	signal := <-sigc
	switch signal {
	case syscall.SIGINT:
		glog.Infof("Got Ctrl-C, shutting down")
	case syscall.SIGTERM:
		glog.Infof("Got SIGTERM, shutting down")
	default:
		glog.Infof("Got signal %d, shutting down", signal)
	}
}