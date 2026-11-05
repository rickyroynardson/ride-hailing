// Identity is a small learning implementation of a central client-credentials issuer.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/lifecycle"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
)

func main() {
	load := func(env string, out any) {
		data, err := os.ReadFile(os.Getenv(env))
		if err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(data, out); err != nil {
			log.Fatal(err)
		}
	}
	var key jwtauth.SigningKey
	var config jwtauth.IssuerConfig
	load("SERVICE_SIGNING_KEY_FILE", &key)
	load("SERVICE_CLIENTS_FILE", &config)
	issuer, err := jwtauth.NewIssuer(key, config)
	if err != nil {
		log.Fatal(err)
	}
	address := os.Getenv("HTTP_ADDR")
	if address == "" {
		address = ":8080"
	}
	tlsConfig, err := workloadtls.ServerConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	life := lifecycle.New(nil)
	server := &http.Server{Addr: address, Handler: life.HTTP(issuer), TLSConfig: tlsConfig, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10}
	admin := &http.Server{Addr: ":9090", Handler: life.Health(nil), ReadHeaderTimeout: time.Second}
	if err := life.Run(lifecycle.Config{HTTP: []*http.Server{server}, Admin: admin, OnReady: func() { log.Print("server ready") }, OnDrain: func() { log.Print("server draining") }}); err != nil {
		if err != lifecycle.ErrDrainDeadline {
			log.Fatal(err)
		}
		log.Print(err)
	}
}
