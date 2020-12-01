package main

import (
	"fmt"
	"net/url"

	"github.com/livepeer/stream-tester/apis/consul"
)

func main() {
	const consulPath = "http://localhost:8500/"
	u, err := url.Parse(consulPath)
	if err != nil {
		panic(err)
	}
	fmt.Printf("url: %s\n", u)
	// rules, err := consul.GetKeyEx(u, "traefik/http/routers/myrouter", true)
	rules, err := consul.GetKeyEx(u, "traefik/http/routers/hall", true)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got rules: %+v\n", rules)
	// err = consul.PutKey(u, "traefik/http/routers/hall/rule", "hall all 2")
	// if err != nil {
	// 	panic(err)
	// }
	err = consul.PutKeys(u, "traefik/http/routers/hall/rule", "hall all 2", "traefik/http/routers/hall/service", "hall service")
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
