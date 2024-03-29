package utils

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"syscall"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2"
)

func ParseFlags(registerVars func(*flag.FlagSet)) {
	flag.Set("logtostderr", "true")

	fs := flag.NewFlagSet("webrtc-load-tester", flag.ExitOnError)

	registerVars(fs)

	_ = fs.String("config", "", "config file (optional)")

	role := os.Args[1]
	err := ff.Parse(fs, os.Args[2:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("LT_WEBRTC"),
		ff.WithEnvVarIgnoreCommas(true),
	)
	if err != nil {
		glog.Errorf("Error parsing args: %v", err)
		os.Exit(2)
	}

	err = flag.CommandLine.Parse(nil)
	if err != nil {
		glog.Errorf("Error parsing args: %v", err)
		os.Exit(2)
	}

	hostName, _ := os.Hostname()
	fmt.Printf("WebRTC Load Tester (%s) version: %s\n", role, model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)
}

func JSONVarFlag(fs *flag.FlagSet, dest interface{}, name, defaultValue, usage string) {
	if err := json.Unmarshal([]byte(defaultValue), dest); err != nil {
		panic(err)
	}
	fs.Func(name, usage, func(s string) error {
		// Clear any previously set value, including the default above
		destVal := reflect.ValueOf(dest).Elem()
		destVal.Set(reflect.Zero(destVal.Type()))

		return json.Unmarshal([]byte(s), dest)
	})
}

func SignalContext() context.Context {
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

func URLVarFlag(fs *flag.FlagSet, dest **url.URL, name, value, usage string) {
	if err := parseURL(value, dest); err != nil {
		panic(err)
	}
	fs.Func(name, usage, func(s string) error {
		return parseURL(s, dest)
	})
}

func parseURL(s string, dest **url.URL) error {
	if s == "" {
		*dest = nil
		return nil
	}

	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if _, err = url.ParseQuery(u.RawQuery); err != nil {
		return err
	}
	*dest = u
	return nil
}
