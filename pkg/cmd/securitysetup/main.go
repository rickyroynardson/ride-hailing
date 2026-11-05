package main

import (
	"flag"
	"github.com/rickyroynardson/ride-hailing/pkg/securitysetup"
	"log"
)

func main() {
	root := flag.String("root", "/state", "mounted security volume layout")
	rotate := flag.Bool("rotate", false, "rotate workload certificates, preserving CA, JWT keys and client secrets")
	flag.Parse()
	if err := securitysetup.Run(*root, *rotate); err != nil {
		log.Fatal(err)
	}
	log.Print("Docker security volumes ready")
}
