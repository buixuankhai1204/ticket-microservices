package domain

import "testing"

func TestNewPaginationClampsLimitToMaxLimit(t *testing.T) {
	p, err := NewPagination(MaxLimit+250, 0)
	if err != nil {
		t.Fatalf("NewPagination returned error: %v", err)
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
