package metrics

import (
	"testing"
)

func TestGetSafeLabel(t *testing.T) {
	r := New()

	if l := r.GetSafeLabel("label1", 2); l != "label1" {
		t.Errorf("Expected to get label1, got %s", l)
	}
	if l := r.GetSafeLabel("label2", 2); l != "label2" {
		t.Errorf("Expected to get label2, got %s", l)
	}
	if l := r.GetSafeLabel("label3", 2); l != "_other_" {
		t.Errorf("Expected to get _other_, got %s", l)
	}

	// Should still be able to get already seen labels
	if l := r.GetSafeLabel("label1", 2); l != "label1" {
		t.Errorf("Expected to get label1 again, got %s", l)
	}

	// Should emit all if max <= 0
	if l := r.GetSafeLabel("label4", 0); l != "label4" {
		t.Errorf("Expected to get label4 with max=0, got %s", l)
	}
}
