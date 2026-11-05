// Package workloadtls loads Docker-provisioned workload identities. There is no plaintext fallback.
package workloadtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var Names = []string{"identity", "user", "ride", "location", "matching", "payment", "gateway", "devconsole"}

func CertificateIdentity(cert *x509.Certificate) (string, error) {
	if cert == nil || len(cert.URIs) != 1 {
		return "", fmt.Errorf("missing workload URI")
	}
	u := cert.URIs[0]
	if u.Scheme != "spiffe" || u.Host != "ride-hailing.local" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return "", fmt.Errorf("invalid workload URI")
	}
	for _, name := range Names {
		if u.Path == "/workload/"+name {
			return name, nil
		}
	}
	return "", fmt.Errorf("unknown workload URI")
}

func load(directory string) (tls.Certificate, *x509.CertPool, error) {
	if directory == "" {
		return tls.Certificate{}, nil, fmt.Errorf("TLS_DIR is required")
	}
	certificate, err := tls.LoadX509KeyPair(filepath.Join(directory, "tls.crt"), filepath.Join(directory, "tls.key"))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pem, err := os.ReadFile(filepath.Join(directory, "ca.crt"))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return tls.Certificate{}, nil, fmt.Errorf("invalid TLS CA bundle")
	}
	return certificate, pool, nil
}

func ClientConfig(directory, serverName string) (*tls.Config, error) {
	if serverName == "" || strings.ContainsAny(serverName, "/:") {
		return nil, fmt.Errorf("expected TLS server name is required")
	}
	certificate, roots, err := load(directory)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate}, ServerName: serverName}, nil
}

func ServerConfig(directory string) (*tls.Config, error) {
	certificate, roots, err := load(directory)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
				return fmt.Errorf("verified client certificate required")
			}
			_, err := CertificateIdentity(state.PeerCertificates[0])
			return err
		}}, nil
}

func ClientConfigFromEnv(serverName string) (*tls.Config, error) {
	return ClientConfig(os.Getenv("TLS_DIR"), serverName)
}
func ServerConfigFromEnv() (*tls.Config, error) { return ServerConfig(os.Getenv("TLS_DIR")) }

func ClientFromEnv(serverName string) (credentials.TransportCredentials, error) {
	config, err := ClientConfigFromEnv(serverName)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(config), nil
}

func ServerFromEnv() (grpc.ServerOption, error) {
	config, err := ServerConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return grpc.Creds(credentials.NewTLS(config)), nil
}

// PeerIdentity reports authenticated TLS identity. Legacy plaintext unit fixtures have no TLS peer; production
// listeners require mTLS, so they cannot take that path. A TLS peer without verified identity is always rejected.
func PeerIdentity(ctx context.Context) (name string, tlsPeer bool, err error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", false, nil
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", false, nil
	}
	if len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return "", true, fmt.Errorf("unverified TLS peer")
	}
	name, err = CertificateIdentity(info.State.PeerCertificates[0])
	return name, true, err
}
