// Securitycheck runs small, non-financial smoke checks from a disposable Compose container.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	identityTLS, err := workloadtls.ClientConfigFromEnv("identity")
	if err != nil {
		return err
	}
	httpTransport := &http.Transport{TLSClientConfig: identityTLS}
	defer httpTransport.CloseIdleConnections()
	httpClient := &http.Client{Transport: httpTransport, Timeout: 3 * time.Second}
	res, err := httpClient.Get("https://identity:8080/token")
	if err != nil {
		return fmt.Errorf("HTTPS identity probe: %w", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed || res.TLS == nil || res.TLS.Version != tls.VersionTLS13 {
		return fmt.Errorf("identity is not serving verified TLS 1.3")
	}
	fmt.Println("PASS identity HTTPS with client certificate (TLS 1.3)")
	fmt.Println("IDENTITY_CERT_SERIAL", res.TLS.PeerCertificates[0].SerialNumber)
	plainResponse, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://identity:8080/token")
	if err == nil {
		plainResponse.Body.Close()
		if plainResponse.StatusCode != http.StatusBadRequest {
			return fmt.Errorf("identity accepted plaintext HTTP")
		}
	}
	fmt.Println("PASS issuer has no plaintext token endpoint")
	plainConn, err := grpcserver.NewClient("payment:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	plainCtx, stopPlain := context.WithTimeout(context.Background(), 3*time.Second)
	plainErr := plainConn.Invoke(plainCtx, "/payment.v1.PaymentService/ChargeRide", &emptypb.Empty{}, &emptypb.Empty{})
	stopPlain()
	plainConn.Close()
	if code := status.Code(plainErr); code != codes.Unavailable && code != codes.DeadlineExceeded {
		return fmt.Errorf("plaintext gRPC was not rejected at transport: %v", plainErr)
	}
	fmt.Println("PASS backend rejects plaintext gRPC")
	creds, err := jwtauth.CredentialsFromEnv("payment")
	if err != nil {
		return err
	}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		return fmt.Errorf("mTLS token exchange: %w", err)
	}
	fmt.Println("PASS service-token exchange over mTLS")
	payTLS, err := workloadtls.ClientConfigFromEnv("payment")
	if err != nil {
		return err
	}
	invoke := func(config *tls.Config, authorization, method string) error {
		conn, err := grpcserver.NewClient("payment:50051", grpc.WithTransportCredentials(credentials.NewTLS(config)))
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if authorization != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", authorization)
		}
		return conn.Invoke(ctx, method, &emptypb.Empty{}, &emptypb.Empty{})
	}
	check := func(label string, config *tls.Config, authorization, method string, want codes.Code) error {
		err := invoke(config, authorization, method)
		if status.Code(err) != want {
			return fmt.Errorf("%s: got %v; want %s", label, err, want)
		}
		fmt.Println("PASS", label)
		return nil
	}
	const charge = "/payment.v1.PaymentService/ChargeRide"
	if err := check("authorized charge reaches payload validation (no financial write)", payTLS, md["authorization"], charge, codes.InvalidArgument); err != nil {
		return err
	}
	if err := check("valid mTLS does not grant refund permission", payTLS, md["authorization"], "/payment.v1.PaymentService/RefundCharge", codes.PermissionDenied); err != nil {
		return err
	}
	if err := check("valid mTLS still requires a JWT", payTLS, "", charge, codes.Unauthenticated); err != nil {
		return err
	}
	wrongAudience, err := jwtauth.CredentialsFromEnv("matching")
	if err != nil {
		return err
	}
	wrongMD, err := wrongAudience.GetRequestMetadata(context.Background())
	if err != nil {
		return err
	}
	if err := check("wrong JWT audience rejected", payTLS, wrongMD["authorization"], charge, codes.Unauthenticated); err != nil {
		return err
	}
	expired, err := tls.LoadX509KeyPair("/checks/expired/tls.crt", "/checks/expired/tls.key")
	if err != nil {
		return err
	}
	mismatch, err := tls.LoadX509KeyPair("/checks/mismatch/tls.crt", "/checks/mismatch/tls.key")
	if err != nil {
		return err
	}
	untrusted, err := untrustedCertificate()
	if err != nil {
		return err
	}
	for _, test := range []struct {
		name   string
		mutate func(*tls.Config)
	}{
		{"missing client certificate", func(c *tls.Config) { c.Certificates = nil }},
		{"untrusted client certificate", func(c *tls.Config) {
			c.Certificates = []tls.Certificate{untrusted}
			c.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &untrusted, nil }
		}},
		{"expired client certificate", func(c *tls.Config) { c.Certificates = []tls.Certificate{expired} }},
		{"wrong server hostname", func(c *tls.Config) { c.ServerName = "user" }},
	} {
		config := payTLS.Clone()
		test.mutate(config)
		err := invoke(config, md["authorization"], charge)
		if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded {
			return fmt.Errorf("%s did not fail at transport: %v", test.name, err)
		}
		fmt.Println("PASS", test.name, "rejected at transport")
	}
	config := payTLS.Clone()
	config.Certificates = []tls.Certificate{mismatch}
	if err := check("JWT service must match the certificate workload", config, md["authorization"], charge, codes.PermissionDenied); err != nil {
		return err
	}
	// Client credentials must match the TLS workload identity at the issuer as well as at gRPC backends.
	config = identityTLS.Clone()
	config.Certificates = []tls.Certificate{mismatch}
	mismatchTransport := &http.Transport{TLSClientConfig: config}
	defer mismatchTransport.CloseIdleConnections()
	data, err := os.ReadFile(os.Getenv("SERVICE_CLIENT_FILE"))
	if err != nil {
		return err
	}
	var client jwtauth.ClientIdentity
	if err := json.Unmarshal(data, &client); err != nil {
		return err
	}
	form := url.Values{"grant_type": {"client_credentials"}, "audience": {"payment"}}
	req, _ := http.NewRequest(http.MethodPost, "https://identity:8080/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(client.ID, client.Secret)
	res, err = (&http.Client{Transport: mismatchTransport, Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		return fmt.Errorf("issuer accepted client secret under a different certificate identity")
	}
	fmt.Println("PASS issuer binds client credentials to certificate identity")
	return nil
}

func untrustedCertificate() (tls.Certificate, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	uri, _ := url.Parse("spiffe://ride-hailing.local/workload/ride")
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "untrusted"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), URIs: []*url.URL{uri}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, err
}
