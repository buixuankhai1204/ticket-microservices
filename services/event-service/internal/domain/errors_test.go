package domain

import (
	"errors"
	"testing"
)

func TestRepositoryErrorWrapsAndUnwraps(t *testing.T) {
	inner := errors.New("connection reset")
	re := &RepositoryError{Err: inner}

	if re.Error() != "repository error: connection reset" {
		t.Fatalf("Error() = %q, want %q", re.Error(), "repository error: connection reset")
	}
	if !errors.Is(re, inner) {
		t.Fatalf("errors.Is(re, inner) = false, want true")
	}
	if errors.Unwrap(re) != inner {
		t.Fatalf("Unwrap() = %v, want %v", errors.Unwrap(re), inner)
	}
}
