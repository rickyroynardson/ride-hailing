// Probe gives distroless images a tiny readiness client; it requires no shell, curl, TLS key or JWT.
package main

import (
	"flag"
	"net/http"
	"os"
	"time"
)

func main() {
	address := flag.String("url", "http://127.0.0.1:8080/readyz", "local readiness URL")
	flag.Parse()
	client := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Get(*address)
	if err != nil {
		os.Exit(1)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
