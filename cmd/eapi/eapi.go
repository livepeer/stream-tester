package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/livepeer/stream-tester/apis/etcd"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2/ffcli"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	rootFlagSet := flag.NewFlagSet("lapi", flag.ExitOnError)
	verbosity := rootFlagSet.String("v", "", "Log verbosity.  {4|5|6}")
	endpointsF := rootFlagSet.String("endpoints", "localhost:2379", "Livepeer API's access token")
	etcdCaCert := rootFlagSet.String("cacert", "", "ETCD CA file name")
	etcdCert := rootFlagSet.String("cert", "", "ETCD client certificate file name")
	etcdKey := rootFlagSet.String("key", "", "ETCD client certificate key file name")
	endpoints := strings.Split(*endpointsF, ",")

	put := &ffcli.Command{
		Name:       "put",
		ShortUsage: "lapi put key_name key_value",
		ShortHelp:  "Puts key",
		Exec: func(_ context.Context, args []string) error {
			if len(args) < 2 {
				return fmt.Errorf("key name and value should be provided")
			}
			eapi, err := etcd.NewEtcd(endpoints, 5*time.Second, *etcdCaCert, *etcdCert, *etcdKey)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if true {
				resp, err := eapi.Client.Put(ctx, args[0], args[1])
				cancel()
				if err != nil {
					return err
				}
				fmt.Printf("key %s val %s revision %d\n", args[0], args[1], resp.Header.Revision)
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				txn := eapi.Client.Txn(ctx)
				cmp := clientv3.Compare(clientv3.CreateRevision(args[0]), "=", 0)
				cmp = clientv3.Compare(clientv3.CreateRevision(args[0]), ">", -1)
				put := clientv3.OpPut(args[0], args[1])
				// reuse key in case this session already holds the lock
				get := clientv3.OpGet(args[0])
				resp, err := txn.If(cmp).Then(put).Else(get).Commit()
				cancel()
				if err != nil {
					return err
				}
				fmt.Printf("Succeeded %v Revision %d\n", resp.Succeeded, resp.Header.Revision)
				// m.myRev = resp.Header.Revision
				if !resp.Succeeded {
					for i, ev := range resp.Responses[0].GetResponseRange().Kvs {
						fmt.Printf("%d: %s : %s CreateRevision: %d ModRevision %d Version %d\n", i, ev.Key,
							ev.Value, ev.CreateRevision, ev.ModRevision, ev.Version)
					}
				}
			}
			return nil
		},
	}

	del := &ffcli.Command{
		Name:       "del",
		ShortUsage: "lapi del key_name",
		ShortHelp:  "Delete key",
		Exec: func(_ context.Context, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("key name should be provided")
			}
			eapi, err := etcd.NewEtcd(endpoints, 5*time.Second, *etcdCaCert, *etcdCert, *etcdKey)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := eapi.Client.Delete(ctx, args[0])
			cancel()
			if err != nil {
				return err
			}
			fmt.Printf("key %s deleted keys %d revision %d\n", args[0], resp.Deleted, resp.Header.Revision)
			return nil
		},
	}

	ls := &ffcli.Command{
		Name:       "ls",
		ShortUsage: "eapi ls",
		ShortHelp:  "Lists keys",
		Exec: func(_ context.Context, args []string) error {
			key := "/"
			if len(args) > 0 {
				key = args[0]
			}
			eapi, err := etcd.NewEtcd(endpoints, 5*time.Second, *etcdCaCert, *etcdCert, *etcdKey) // hardcode AC server for now
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var opts []clientv3.OpOption
			if len(args) > 1 {
				opts = append(opts, clientv3.WithRange(args[1]))
			} else {
				opts = append(opts, clientv3.WithFromKey())
			}
			resp, err := eapi.Client.Get(ctx, key, opts...)
			cancel()
			if err != nil {
				return err
			}
			if len(resp.Kvs) == 0 {
				fmt.Printf("No keys found for %s\n", key)
			}
			for _, ev := range resp.Kvs {
				fmt.Printf("'%s'  CreateRevision: %d ModRevision %d Version %d\n", ev.Key, ev.CreateRevision,
					ev.ModRevision, ev.Version)
			}
			return nil
		},
	}

	root := &ffcli.Command{
		ShortUsage:  "eapi [flags] <subcommand>",
		FlagSet:     rootFlagSet,
		Subcommands: []*ffcli.Command{put, ls, del},
	}

	if err := root.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)
	hostName, _ := os.Hostname()
	fmt.Println("eapi version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())

	if err := root.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
