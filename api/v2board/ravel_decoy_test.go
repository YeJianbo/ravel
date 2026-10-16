package panel

import "testing"

func TestRavelWebSocketMasqueradeValidation(t *testing.T) {
	for _, raw := range []string{"", "ws://127.0.0.1:8080/application?mode=cover", "wss://cover.example/app"} {
		config := validRavelCommonNode()
		config.Ravel.WebSocketMasquerade = raw
		if err := validateRavelNode(config); err != nil {
			t.Errorf("valid %q: %v", raw, err)
		}
	}
	for _, raw := range []string{"https://cover.example/app", "ws://user:secret@cover.example/", "ws://cover.example/#fragment", "ws:///missing", "ws://cover.example/\n"} {
		config := validRavelCommonNode()
		config.Ravel.WebSocketMasquerade = raw
		if err := validateRavelNode(config); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
}

func TestRavelUnauthenticatedWorkLimits(t *testing.T) {
	for _, limit := range []int{-1, 65537} {
		config := validRavelCommonNode()
		config.Ravel.Settings.MaxConnections = limit
		if err := validateRavelNode(config); err == nil {
			t.Errorf("accepted connection limit %d", limit)
		}
		config.Ravel.Settings.MaxConnections = 0
		config.Ravel.Settings.MaxAdmissions = limit
		if err := validateRavelNode(config); err == nil {
			t.Errorf("accepted admission limit %d", limit)
		}
	}
}
