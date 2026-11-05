package securitysetup_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/rickyroynardson/ride-hailing/pkg/securitysetup"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
)

func TestProvisionIdempotencyIsolationAndRotation(t *testing.T) {
	root := t.TempDir()
	if err := securitysetup.Run(root, false); err != nil {
		t.Fatal(err)
	}
	read := func(path string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := map[string][]byte{}
	for _, path := range []string{"ca/ca.key", "ca/ca.crt", "ride/client.json", "user/signing.json", "identity/signing.json", "ride/tls.crt", "ride/tls.key"} {
		before[path] = read(path)
	}
	if err := securitysetup.Run(root, false); err != nil {
		t.Fatal(err)
	}
	for path, data := range before {
		if !bytes.Equal(data, read(path)) {
			t.Fatalf("init overwrote %s", path)
		}
	}
	for _, name := range workloadtls.Names {
		for _, file := range []string{"ca.key", "user-signing.json", "service-signing.json"} {
			if _, err := os.Stat(filepath.Join(root, name, file)); !os.IsNotExist(err) {
				t.Fatalf("unexpected shared private material %s/%s", name, file)
			}
		}
		info, err := os.Stat(filepath.Join(root, name, "tls.key"))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private key permissions: %v %v", info, err)
		}
	}
	if err := securitysetup.Run(root, true); err != nil {
		t.Fatal(err)
	}
	for path, data := range before {
		changed := !bytes.Equal(data, read(path))
		if want := path == "ride/tls.crt" || path == "ride/tls.key"; changed != want {
			t.Fatalf("rotation changed=%v for %s", changed, path)
		}
	}
	// Expired/broken workload certificates fail startup rather than silently bypassing TLS. Rotation repairs them
	// without discarding the application credentials or CA.
	if err := os.WriteFile(filepath.Join(root, "ride", "tls.crt"), read("checks/expired/tls.crt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := securitysetup.Run(root, false); err == nil {
		t.Fatal("broken existing TLS state accepted")
	}
	if err := securitysetup.Run(root, true); err != nil {
		t.Fatal(err)
	}
}

func TestPartialSecurityStateIsNotOverwritten(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "ride"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "ride", "client.json")
	if err := os.WriteFile(path, []byte("existing credential"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := securitysetup.Run(root, false); err == nil {
		t.Fatal("partial setup accepted")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "existing credential" {
		t.Fatal("existing state overwritten")
	}
}
