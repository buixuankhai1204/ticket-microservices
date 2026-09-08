package domain

import (
	"errors"
	"testing"
)

func TestNewPaginationValid(t *testing.T) {
	p, err := NewPagination(25, 40)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Limit != 25 || p.Offset != 40 {
		t.Fatalf("got %+v, want {Limit:25 Offset:40}", p)
	}
}

func TestNewPaginationRejectsNegativeOffset(t *testing.T) {
	p, err := NewPagination(20, -1)
	if !errors.Is(err, ErrInvalidPagination) {
		t.Fatalf("err = %v, want errors.Is %v", err, ErrInvalidPagination)
	}
	if p != (Pagination{}) {
		t.Fatalf("pagination = %+v, want zero value on error", p)
	}
}

func TestNewPaginationRejectsLimitBelowOne(t *testing.T) {
	_, err := NewPagination(0, 0)
	if !errors.Is(err, ErrInvalidPagination) {
		t.Fatalf("err = %v, want errors.Is %v", err, ErrInvalidPagination)
	}
}

func TestNewPaginationClampsLimitToMax(t *testing.T) {
	p, err := NewPagination(MaxLimit+250, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Limit != MaxLimit {
		t.Fatalf("Limit = %d, want clamped to %d", p.Limit, MaxLimit)
	}
}

func TestPaginationHasMore(t *testing.T) {
	tests := []struct {
		name    string
		offset  int
		pageLen int
		total   int
		want    bool
	}{
		{name: "rows remain after this page", offset: 0, pageLen: 20, total: 50, want: true},
		{name: "final page", offset: 40, pageLen: 10, total: 50, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := Pagination{Offset: tc.offset}
			if got := p.HasMore(tc.pageLen, tc.total); got != tc.want {
				t.Fatalf("HasMore(%d, %d) with offset %d = %v, want %v", tc.pageLen, tc.total, tc.offset, got, tc.want)
			}
		})
	}
}

func TestPaginationConstants(t *testing.T) {
	if DefaultLimit != 20 {
		t.Fatalf("DefaultLimit = %d, want 20", DefaultLimit)
	}
	if MaxLimit != 100 {
		t.Fatalf("MaxLimit = %d, want 100", MaxLimit)
	}
}
