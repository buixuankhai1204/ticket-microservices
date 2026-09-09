package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestRepositoryErrorMessageAndUnwrap(t *testing.T) {
	inner := errors.New("connection refused")
	re := &RepositoryError{Err: inner}

	if got := re.Error(); got != "repository error: connection refused" {
		t.Fatalf("Error() = %q, want %q", got, "repository error: connection refused")
	}
	if errors.Unwrap(re) != inner {
		t.Fatalf("Unwrap() = %v, want %v", errors.Unwrap(re), inner)
	}
	if !errors.Is(re, inner) {
		t.Fatalf("errors.Is(re, inner) = false, want true")
	}

	wrapped := &RepositoryError{Err: fmt.Errorf("query failed: %w", ErrNotFound)}
	if !errors.Is(wrapped, ErrNotFound) {
		t.Fatalf("errors.Is(wrapped, ErrNotFound) = false, want true")
	}
}
