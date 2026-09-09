package domain

import (
	"errors"
	"testing"
)

func TestNewPaginationHappyPath(t *testing.T) {
	p, err := NewPagination(25, 40)
	if err != nil {
		t.Fatalf("NewPagination returned error: %v", err)
	}
	if p.Limit != 25 || p.Offset != 40 {
		t.Fatalf("got %+v, want {Limit:25 Offset:40}", p)
	}
}

func TestNewPaginationRejectsOutOfRangeArguments(t *testing.T) {
	tests := []struct {
		name   string
		limit  int
		offset int
	}{
		{name: "negative offset", limit: 20, offset: -1},
		{name: "limit below one", limit: 0, offset: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewPagination(tc.limit, tc.offset)
			if !errors.Is(err, ErrInvalidPagination) {
				t.Fatalf("err = %v, want ErrInvalidPagination", err)
			}
			if p != (Pagination{}) {
				t.Fatalf("pagination = %+v, want zero value on error", p)
			}
		})
	}
}

func TestNewPaginationClampsLimitToMaxLimit(t *testing.T) {
	p, err := NewPagination(MaxLimit+250, 0)
	if err != nil {
		t.Fatalf("NewPagination returned error: %v", err)
	}
	if p.Limit != MaxLimit {
		t.Fatalf("Limit = %d, want clamped to %d", p.Limit, MaxLimit)
	}
}

func TestNewPaginationLeavesDefaultLimitUnchanged(t *testing.T) {
	p, err := NewPagination(DefaultLimit, 0)
	if err != nil {
		t.Fatalf("NewPagination returned error: %v", err)
	}
	if p.Limit != DefaultLimit || p.Offset != 0 {
		t.Fatalf("got %+v, want {Limit:%d Offset:0}", p, DefaultLimit)
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
