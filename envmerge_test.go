package main

import (
	"os"
	"testing"

	"github.com/joho/godotenv"
)

func TestUpdateEnvKeepsOtherKeys(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	defer func() { _ = os.Chdir(wd) }()
	_ = os.Chdir(dir)
	if err := os.WriteFile(".env", []byte("MDNS_ZEROCONF=1\nSMGW_TIME=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := updateEnv(map[string]string{"CERT_PEM": "-----BEGIN X-----\nabc\n-----END X-----\n"}); err != nil {
		t.Fatal(err)
	}
	env, err := godotenv.Read(".env")
	if err != nil {
		t.Fatal(err)
	}
	if env["MDNS_ZEROCONF"] != "1" || env["SMGW_TIME"] != "0" || env["CERT_PEM"] == "" {
		t.Fatalf("settings lost: %v", env)
	}
	if err := updateEnv(map[string]string{"SMGW_TIME": ""}); err != nil {
		t.Fatal(err)
	}
	env, _ = godotenv.Read(".env")
	if _, ok := env["SMGW_TIME"]; ok || env["MDNS_ZEROCONF"] != "1" {
		t.Fatalf("delete failed: %v", env)
	}
}
