// Package securitysetup provisions local-development security into per-workload Docker volumes.
// Only the one-shot provisioning container mounts the CA private volume. Workloads mount their own volume read-only.
package securitysetup

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
)

var clients = []string{"ride", "matching", "payment", "devconsole"}

func Run(root string, rotate bool) error {
	marker := filepath.Join(root, "ca", "complete")
	_, err := os.Stat(marker)
	existing := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if !existing {
		if rotate {
			return fmt.Errorf("security is not initialized")
		}
		for _, name := range append([]string{"ca", "checks"}, workloadtls.Names...) {
			entries, err := os.ReadDir(filepath.Join(root, name))
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			if len(entries) != 0 {
				return fmt.Errorf("incomplete security state in %s; inspect it or use scoped security cleanup", name)
			}
		}
		if err := initialize(root); err != nil {
			return err
		}
	}
	caPEM, err := os.ReadFile(filepath.Join(root, "ca", "ca.crt"))
	if err != nil {
		return err
	}
	block, _ := pem.Decode(caPEM)
	if block == nil {
		return fmt.Errorf("invalid CA certificate")
	}
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	keyPEM, err := os.ReadFile(filepath.Join(root, "ca", "ca.key"))
	if err != nil {
		return err
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return fmt.Errorf("invalid CA key")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return err
	}
	key, ok := parsed.(ed25519.PrivateKey)
	public, publicOK := ca.PublicKey.(ed25519.PublicKey)
	if !ok || !publicOK || !bytes.Equal(key.Public().(ed25519.PublicKey), public) {
		return fmt.Errorf("CA key mismatch")
	}
	if !ca.IsCA || time.Until(ca.NotAfter) < 7*24*time.Hour {
		return fmt.Errorf("CA expires too soon; provision a new local CA with scoped security cleanup")
	}
	if !existing || rotate {
		for _, name := range workloadtls.Names {
			if err := leaf(root, name, name, ca, key, caPEM, false); err != nil {
				return err
			}
		}
		// Negative-test credentials never appear in application volumes. No CA private key is given to the checker.
		if err := leaf(root, "checks/expired", "ride", ca, key, caPEM, true); err != nil {
			return err
		}
		if err := leaf(root, "checks/mismatch", "location", ca, key, caPEM, false); err != nil {
			return err
		}
	}
	if err := validate(root, caPEM); err != nil {
		return err
	}
	if !existing {
		return write(root, "ca", "complete", []byte("security-v1\n"))
	}
	return nil
}

func initialize(root string) error {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "ride-hailing local CA"}, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(365 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, MaxPathLenZero: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		return err
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return err
	}
	if err := write(root, "ca", "ca.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})); err != nil {
		return err
	}
	if err := write(root, "ca", "ca.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})); err != nil {
		return err
	}
	newKey := func(id string) (jwtauth.SigningKey, string, error) {
		pub, private, err := ed25519.GenerateKey(rand.Reader)
		return jwtauth.SigningKey{ID: id, PrivateKey: base64.StdEncoding.EncodeToString(private)}, base64.StdEncoding.EncodeToString(pub), err
	}
	user, userPublic, err := newKey("user-1")
	if err != nil {
		return err
	}
	service, servicePublic, err := newKey("service-1")
	if err != nil {
		return err
	}
	bundle := jwtauth.TrustBundle{User: map[string]string{user.ID: userPublic}, Service: map[string]string{service.ID: servicePublic}}
	for _, name := range workloadtls.Names {
		if err := writeJSON(root, name, "trust.json", bundle); err != nil {
			return err
		}
	}
	if err := writeJSON(root, "user", "signing.json", user); err != nil {
		return err
	}
	if err := writeJSON(root, "identity", "signing.json", service); err != nil {
		return err
	}
	registry := jwtauth.IssuerConfig{Clients: make(map[string]string)}
	for _, name := range clients {
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return err
		}
		encoded := base64.RawURLEncoding.EncodeToString(secret)
		if err := writeJSON(root, name, "client.json", jwtauth.ClientIdentity{ID: name, Secret: encoded}); err != nil {
			return err
		}
		registry.Clients[name] = jwtauth.SecretDigest(encoded)
	}
	return writeJSON(root, "identity", "clients.json", registry)
}

func leaf(root, directory, name string, ca *x509.Certificate, key ed25519.PrivateKey, caPEM []byte, expired bool) error {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	u, _ := url.Parse("spiffe://ride-hailing.local/workload/" + name)
	now := time.Now()
	start, end := now.Add(-time.Minute), now.Add(7*24*time.Hour)
	if expired {
		start, end = now.Add(-2*time.Hour), now.Add(-time.Hour)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, URIs: []*url.URL{u}, NotBefore: start, NotAfter: end, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, public, key)
	if err != nil {
		return err
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return err
	}
	for file, data := range map[string][]byte{"tls.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), "tls.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), "ca.crt": caPEM} {
		if err := write(root, directory, file, data); err != nil {
			return err
		}
	}
	return nil
}

func validate(root string, caPEM []byte) error {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("invalid CA")
	}
	for _, name := range workloadtls.Names {
		dir := filepath.Join(root, name)
		pair, err := tls.LoadX509KeyPair(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
		if err != nil {
			return err
		}
		cert, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return err
		}
		if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: name}); err != nil {
			return fmt.Errorf("%s TLS identity invalid; run certificate rotation: %w", name, err)
		}
		identity, err := workloadtls.CertificateIdentity(cert)
		if err != nil || identity != name {
			return fmt.Errorf("%s certificate has wrong workload identity", name)
		}
		publicCA, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
		if err != nil || !bytes.Equal(publicCA, caPEM) {
			return fmt.Errorf("%s CA bundle mismatch", name)
		}
		var bundle jwtauth.TrustBundle
		if err := readJSON(filepath.Join(dir, "trust.json"), &bundle); err != nil {
			return err
		}
		if _, err := jwtauth.NewTokenVerifier(name, bundle); err != nil {
			return err
		}
	}
	var signer jwtauth.SigningKey
	var registry jwtauth.IssuerConfig
	if err := readJSON(filepath.Join(root, "identity", "signing.json"), &signer); err != nil {
		return err
	}
	if err := readJSON(filepath.Join(root, "identity", "clients.json"), &registry); err != nil {
		return err
	}
	if _, err := jwtauth.NewIssuer(signer, registry); err != nil {
		return err
	}
	if err := readJSON(filepath.Join(root, "user", "signing.json"), &signer); err != nil {
		return err
	}
	if _, err := jwtauth.NewUserSigner(signer); err != nil {
		return err
	}
	for _, name := range clients {
		var client jwtauth.ClientIdentity
		if err := readJSON(filepath.Join(root, name, "client.json"), &client); err != nil {
			return err
		}
		if client.ID != name || len(client.Secret) < 32 || registry.Clients[name] != jwtauth.SecretDigest(client.Secret) {
			return fmt.Errorf("%s client registry mismatch", name)
		}
	}
	return nil
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}
func writeJSON(root, directory, file string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return write(root, directory, file, data)
}

func write(root, directory, file string, data []byte) error {
	dir := filepath.Join(root, directory)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return err
	}
	uid, gid := os.Getuid(), os.Getgid()
	if directory != "ca" && os.Geteuid() == 0 {
		uid, gid = 65532, 65532
	}
	if err := os.Chown(dir, uid, gid); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".provision-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Chown(uid, gid); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(dir, file))
}
