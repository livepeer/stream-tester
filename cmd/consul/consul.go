package main

import (
	"flag"
	"fmt"
	"net/url"

	"github.com/livepeer/stream-tester/apis/consul"
)

const consulPath = "http://localhost:8500/"

func rewriteTest(u *url.URL) {
	err := consul.PutKeys(u, "uroboros 1", "bit 1", "uroboros 2", "eaten")
	if err != nil {
		panic(err)
	}
	keys, err := consul.GetKeyEx(u, "uroboros 1", false)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got keys1 2: %+v\n", keys)
}

func rewriteTestWithTime(u *url.URL) {
	err := consul.PutKeysWithCurrentTime(u, "uroboros 1", "bit 1", "uroboros 2", "eaten")
	if err != nil {
		panic(err)
	}
	keys, err := consul.GetKeyEx(u, "uroboros 1", false)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got keys1 2: %+v\n", keys)
}

func removeWithCas(u *url.URL) {
	keys, err := consul.GetKeyEx(u, "uroboros 1", false)
	if err == consul.ErrNotFound {
		fmt.Printf("Key not found!\n")
		return
	}
	if err != nil {
		panic(err)
	}
	k1 := keys[0]
	// k1.ModifyIndex--
	keys[0] = k1
	fmt.Printf("Got keys1 2: %+v\n", keys)
	dr, err := consul.DeleteKeysCas(u, keys)
	if err == consul.ErrConfilct {
		fmt.Printf("Conflict deleting key!\n")
	} else if err != nil {
		panic(err)
	}
	fmt.Printf("Delete result: %v\n", dr)
}

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")
	// verbosity := flag.String("v", "", "Log verbosity.  {4|5|6}")
	flag.Parse()
	// vFlag.Value.Set(*verbosity)
	fmt.Printf("verbosity is %v ", *&vFlag.Value)
	u, err := url.Parse(consulPath)
	if err != nil {
		panic(err)
	}

	fmt.Printf("url: %s\n", u)
	// rewriteTest(u)
	// rewriteTestWithTime(u)
	removeWithCas(u)
	return

	/*
		res, err := consul.DeleteKey(u, "traefik/http/routers/hall/rule", true)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Delete result: %v\n", res)
		return
	*/

	// rules, err := consul.GetKeyEx(u, "traefik/http/routers/myrouter", true)
	rules, err := consul.GetKeyEx(u, "traefik/http/routers/hall", true)
	if err != nil {
		// panic(err)
	}
	fmt.Printf("Got rules: %+v\n", rules)
	// err = consul.PutKey(u, "traefik/http/routers/hall/rule", "hall all 2")
	// if err != nil {
	// 	panic(err)
	// }
	err = consul.PutKeys(u, "traefik/http/routers/hall/rule", "hall all 3", "traefik/http/routers/hall/service", "hall service")
	if err != nil {
		panic(err)
	}
	rules, err = consul.GetKeyEx(u, "traefik/http/routers/hall", true)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got rules 2: %+v\n", rules)
	rule, err := consul.GetKey(u, "traefik/http/routers/hall/service")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got service: '%s'\n", rule)
	return
	res, err := consul.DeleteKey(u, "traefik/http/routers/hall/rule", false)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Delete result: %v\n", res)
	rule, err = consul.GetKey(u, "traefik/http/routers/hall/rule")
	if err != nil && err != consul.ErrNotFound {
		panic(err)
	}
	fmt.Printf("Got rule: '%s'\n", rule)
	res, err = consul.DeleteKey(u, "traefik/http/routers/hall/rule", true)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Delete result: %v\n", res)
}
