package domain_test

import (
	"errors"
	"testing"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

func TestPaginationRejectsNonsenseAndClampsOversizedPages(t *testing.T) {
	tests := []struct {
		name      string
		limit     int
		offset    int
		wantLimit int
		wantErr   error
	}{
		{"typical page", 20, 40, 20, nil},
		{"smallest page", 1, 0, 1, nil},
		{"largest allowed page", domain.MaxLimit, 0, domain.MaxLimit, nil},
		{"oversized page is clamped, not rejected", domain.MaxLimit + 250, 0, domain.MaxLimit, nil},
		{"zero limit", 0, 0, 0, domain.ErrInvalidPagination},
		{"negative offset", 10, -1, 0, domain.ErrInvalidPagination},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := domain.NewPagination(tc.limit, tc.offset)

			if !errors.Is(err, tc.wantErr) || p.Limit != tc.wantLimit {
				t.Fatalf("got (%+v, %v), want limit %d and error %v", p, err, tc.wantLimit, tc.wantErr)
			}
		})
	}
}

func TestHasMoreIsTrueOnlyWhenRowsRemainBeyondThisPage(t *testing.T) {
	tests := []struct {
		name    string
		offset  int
		pageLen int
		total   int
		want    bool
	}{
		{"first of three pages", 0, 20, 50, true},
		{"last page is short", 40, 10, 50, false},
		{"last page is exactly full", 30, 20, 50, false},
		{"empty result", 0, 0, 0, false},
		{"offset beyond the end", 100, 0, 50, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (domain.Pagination{Offset: tc.offset}).HasMore(tc.pageLen, tc.total); got != tc.want {
				t.Fatalf("HasMore(%d, %d) at offset %d = %v, want %v", tc.pageLen, tc.total, tc.offset, got, tc.want)
			}
		})
	}
}
