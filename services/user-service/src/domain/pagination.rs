use super::errors::UserError;

/// Default page size when the client sends no `limit`.
pub const DEFAULT_LIMIT: i64 = 20;
/// Upper bound on `limit`. A larger request is clamped down to this, not rejected.
pub const MAX_LIMIT: i64 = 100;

/// Validated pagination for every list endpoint in this service. Built once via
/// [`Pagination::new`], never constructed field-by-field, so the invariants
/// below hold everywhere a `Pagination` is seen.
#[derive(Debug, Clone, Copy)]
pub struct Pagination {
    pub limit: i64,
    pub offset: i64,
}

impl Pagination {
    /// Rejects `offset < 0` or `limit < 1` with a domain error (an absent query
    /// param is defaulted by the HTTP layer *before* it gets here; a
    /// present-but-invalid value is a 400). `limit` above [`MAX_LIMIT`] is
    /// clamped down rather than rejected.
    pub fn new(limit: i64, offset: i64) -> Result<Self, UserError> {
        if offset < 0 || limit < 1 {
            return Err(UserError::InvalidPagination);
        }
        Ok(Self {
            limit: limit.min(MAX_LIMIT),
            offset,
        })
    }

    /// Whether more rows exist past this page, given the full match count.
    pub fn has_more(&self, page_len: usize, total: i64) -> bool {
        self.offset + (page_len as i64) < total
    }
}
