package workloadtls_test

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/securitysetup"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
)

func TestMutualTLSVerification(t *testing.T) {
	root := t.TempDir()
	if err := securitysetup.Run(root, false); err != nil {
		t.Fatal(err)
	}
	serverConfig, err := workloadtls.ServerConfig(filepath.Join(root, "payment"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, err := workloadtls.CertificateIdentity(r.TLS.PeerCertificates[0])
		if err != nil || name != "ride" {
			t.Error("incorrect peer identity", name, err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverConfig
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	clientConfig, err := workloadtls.ClientConfig(filepath.Join(root, "ride"), "payment")
	if err != nil {
		t.Fatal(err)
	}
	expired, err := tls.LoadX509KeyPair(filepath.Join(root, "checks/expired/tls.crt"), filepath.Join(root, "checks/expired/tls.key"))
	if err != nil {
		t.Fatal(err)
	}
	otherRoot := t.TempDir()
	if err := securitysetup.Run(otherRoot, false); err != nil {
		t.Fatal(err)
	}
	untrusted, err := tls.LoadX509KeyPair(filepath.Join(otherRoot, "ride/tls.crt"), filepath.Join(otherRoot, "ride/tls.key"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		mutate  func(*tls.Config)
		success bool
	}{
		{"valid", func(*tls.Config) {}, true},
		{"missing client certificate", func(c *tls.Config) { c.Certificates = nil }, false},
		{"untrusted client certificate", func(c *tls.Config) {
			c.Certificates = []tls.Certificate{untrusted}
			c.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &untrusted, nil }
		}, false},
		{"expired client certificate", func(c *tls.Config) { c.Certificates = []tls.Certificate{expired} }, false},
		{"wrong server identity", func(c *tls.Config) { c.ServerName = "user" }, false},
		{"untrusted server CA", func(c *tls.Config) { c.RootCAs = x509.NewCertPool() }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := clientConfig.Clone()
			test.mutate(config)
			transport := &http.Transport{TLSClientConfig: config}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
			response, err := client.Get(server.URL)
			if response != nil {
				response.Body.Close()
			}
			if (err == nil) != test.success {
				t.Fatalf("success=%v err=%v", test.success, err)
			}
		})
	}
	// Verify expiry on the server side too, rather than testing only client-certificate expiry.
	expiredServer := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("expired server reached handler") }))
	expiredServer.TLS, err = workloadtls.ServerConfig(filepath.Join(root, "checks/expired"))
	if err != nil {
		t.Fatal(err)
	}
	expiredServer.Config.ErrorLog = log.New(io.Discard, "", 0)
	expiredServer.StartTLS()
	defer expiredServer.Close()
	config := clientConfig.Clone()
	config.ServerName = "ride"
	transport := &http.Transport{TLSClientConfig: config}
	defer transport.CloseIdleConnections()
	if response, err := (&http.Client{Transport: transport, Timeout: 2 * time.Second}).Get(expiredServer.URL); err == nil {
		response.Body.Close()
		t.Fatal("expired server accepted")
	}
}
