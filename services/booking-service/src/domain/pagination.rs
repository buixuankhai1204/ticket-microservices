use super::errors::BookingError;

pub const DEFAULT_LIMIT: i64 = 20;
pub const MAX_LIMIT: i64 = 100;

#[derive(Debug, Clone, Copy)]
pub struct Pagination {
    pub limit: i64,
    pub offset: i64,
}

impl Pagination {
    pub fn new(limit: i64, offset: i64) -> Result<Self, BookingError> {
        if offset < 0 || limit < 1 {
            return Err(BookingError::InvalidPagination);
        }
        Ok(Self {
            limit: limit.min(MAX_LIMIT),
            offset,
        })
    }

    pub fn has_more(&self, page_len: usize, total: i64) -> bool {
        self.offset.saturating_add(page_len as i64) < total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_clamps_limit_above_max_down_to_max() {
        assert_eq!(Pagination::new(MAX_LIMIT + 1, 0).unwrap().limit, MAX_LIMIT);
        assert_eq!(Pagination::new(i64::MAX, 0).unwrap().limit, MAX_LIMIT);
    }

    #[test]
    fn has_more_is_exact_at_the_boundary_and_never_overflows() {
        let p = Pagination::new(20, 40).unwrap();
        assert!(p.has_more(9, 50));
        assert!(!p.has_more(10, 50));

        let p = Pagination::new(10, i64::MAX).unwrap();
        assert!(!p.has_more(5, i64::MAX));
    }
}
