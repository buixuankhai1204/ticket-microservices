use super::errors::UserError;

pub const DEFAULT_LIMIT: i64 = 20;
pub const MAX_LIMIT: i64 = 100;

#[derive(Debug, Clone, Copy)]
pub struct Pagination {
    pub limit: i64,
    pub offset: i64,
}

impl Pagination {
    pub fn new(limit: i64, offset: i64) -> Result<Self, UserError> {
        if offset < 0 || limit < 1 {
            return Err(UserError::InvalidPagination);
        }
        Ok(Self {
            limit: limit.min(MAX_LIMIT),
            offset,
        })
    }

    pub fn has_more(&self, page_len: usize, total: i64) -> bool {
        self.offset + (page_len as i64) < total
    }
}
