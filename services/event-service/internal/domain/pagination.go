package domain

const (
	DefaultLimit = 20
	MaxLimit     = 100
)

type Pagination struct {
	Limit  int
	Offset int
}

func NewPagination(limit, offset int) (Pagination, error) {
	if offset < 0 || limit < 1 {
		return Pagination{}, ErrInvalidPagination
	}
	if limit > MaxLimit {
		limit = MaxLimit
	}
	return Pagination{Limit: limit, Offset: offset}, nil
}

func (p Pagination) HasMore(pageLen, total int) bool {
	return p.Offset+pageLen < total
}
