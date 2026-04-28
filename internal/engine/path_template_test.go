package engine

import (
	"testing"
)

func TestMatchPathTemplate(t *testing.T) {
	templates := ParsePathTemplates([]string{
		"/users/{id}",
		"/users/{id}/orders",
		"/orders/{abc}",
	})

	tests := []struct {
		input    string
		expected string
	}{
		{"/users/123", "/users/{id}"},
		{"/users/abc/orders", "/users/{id}/orders"},
		{"/orders/xyz", "/orders/{abc}"},
		{"/", "/"},                         // no match
		{"/users", "/users"},               // no match
		{"/unknown/path", "/unknown/path"}, // no match
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			actual := MatchPathTemplate(tt.input, templates)
			if actual != tt.expected {
				t.Errorf("MatchPathTemplate(%q) = %q, expected %q", tt.input, actual, tt.expected)
			}
		})
	}
}

func BenchmarkMatchPathTemplate(b *testing.B) {
	templates := ParsePathTemplates([]string{
		"/users/{id}",
		"/users/{id}/orders",
		"/orders/{abc}",
	})
	path := "/users/123/orders"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MatchPathTemplate(path, templates)
	}
}

func BenchmarkMatchPathTemplateMismatch(b *testing.B) {
	templates := ParsePathTemplates([]string{
		"/users/{id}",
		"/users/{id}/orders",
		"/orders/{abc}",
	})
	path := "/users/123/orders/xyz/123"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MatchPathTemplate(path, templates)
	}
}
