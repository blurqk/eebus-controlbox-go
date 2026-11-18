package main

import "testing"

func TestUnescapeDNS(t *testing.T) {
	for in, want := range map[string]string{
		`brand=Weidm\195\188ller`: "brand=Weidmüller",
		`a\.b`:                    "a.b",
		"plain":                   "plain",
	} {
		if got := unescapeDNS(in); got != want {
			t.Errorf("unescapeDNS(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNum(t *testing.T) {
	cases := []struct {
		v    float64
		want [2]int64
	}{{4200, [2]int64{4200, 0}}, {-14820, [2]int64{-14820, 0}}, {49.98, [2]int64{4998, -2}}, {1e15, [2]int64{1000000000000, 0}}}
	for _, c := range cases {
		got := num(c.v)
		if got[0] != c.want[0] || got[1] != c.want[1] {
			t.Errorf("num(%v) = %v, want %v", c.v, got, c.want)
		}
	}
}

func TestParsePairingQRSpacedSki(t *testing.T) {
	qr := "SHIP;SKI:3976 5FF1 0000 0000 0000 0000 0000 0000 0000 0001;ID:i:1_u:X;FPH256:" +
		"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff;SPSEC:00112233445566778899aabbccddeeff;ENDSHIP;"
	target, err := parsePairingQR(qr)
	if err != nil {
		t.Fatal(err)
	}
	if target.SKI != "39765ff100000000000000000000000000000001" {
		t.Fatalf("SKI = %q", target.SKI)
	}
}
