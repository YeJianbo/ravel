package agent

import "testing"

func TestPublicAddressValidatesActualFamilyAndRejectsPrivateIPs(t *testing.T) {
	for _, item := range []struct{ ip, family, want string }{
		{"23.141.12.117", "4", "23.141.12.117"},
		{"2400:c620:2f:74::a", "6", "2400:c620:2f:74::a"},
		{"23.141.12.117", "6", ""},
		{"2400:c620:2f:74::a", "4", ""},
		{"10.0.0.1", "4", ""},
		{"100.64.0.1", "4", ""},
		{"198.18.0.1", "4", ""},
		{"fc00::1", "6", ""},
		{"not-an-ip", "4", ""},
	} {
		if got := validPublicAddress(item.ip, item.family); got != item.want {
			t.Fatalf("%s family %s: got %q, want %q", item.ip, item.family, got, item.want)
		}
	}
}
