package domain

// Pagination values for every list endpoint in this service. Defined once here,
// not redefined per entity (CLAUDE.md: "a shared Pagination domain type per
// service").
const (
	// DefaultLimit is applied when the client sends no limit at all.
	DefaultLimit = 20
	// MaxLimit caps limit — a larger request is clamped down, not rejected.
	MaxLimit = 100
)

// Pagination is an already-validated limit/offset pair. Construct it only via
// NewPagination so the invariants below hold everywhere.
type Pagination struct {
	Limit  int
	Offset int
}

// NewPagination validates and normalises a limit/offset pair. offset must be
// >= 0 and limit >= 1 (an absent param is defaulted by the HTTP layer before it
// gets here; a present-but-invalid value is a 400). limit above MaxLimit is
// clamped down rather than rejected.
func NewPagination(limit, offset int) (Pagination, error) {
	if offset < 0 || limit < 1 {
		return Pagination{}, ErrInvalidPagination
	}
	if limit > MaxLimit {
		limit = MaxLimit
	}
	return Pagination{Limit: limit, Offset: offset}, nil
}

// HasMore reports whether more rows exist beyond the current page, given the
// full match count.
func (p Pagination) HasMore(pageLen, total int) bool {
	return p.Offset+pageLen < total
}
