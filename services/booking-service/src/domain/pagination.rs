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
    fn new_keeps_valid_limit_and_offset_unchanged() {
        let p = Pagination::new(25, 40).unwrap();
        assert_eq!(p.limit, 25);
        assert_eq!(p.offset, 40);
    }

    #[test]
    fn new_rejects_negative_offset() {
        let err = Pagination::new(20, -1).unwrap_err();
        assert!(matches!(err, BookingError::InvalidPagination));
    }

    #[test]
    fn new_rejects_limit_below_one() {
        let err = Pagination::new(0, 0).unwrap_err();
        assert!(matches!(err, BookingError::InvalidPagination));
    }

    #[test]
    fn new_clamps_limit_to_max() {
        let p = Pagination::new(MAX_LIMIT + 1, 0).unwrap();
        assert_eq!(p.limit, MAX_LIMIT);
    }

    #[test]
    fn has_more_is_true_when_rows_remain_after_this_page() {
        let p = Pagination::new(20, 0).unwrap();
        assert!(p.has_more(20, 50));
    }

    #[test]
    fn has_more_is_false_on_the_final_page() {
        let p = Pagination::new(20, 40).unwrap();
        assert!(!p.has_more(10, 50));
    }

    #[test]
    fn has_more_saturates_instead_of_overflowing_near_i64_max() {
        let p = Pagination::new(10, i64::MAX).unwrap();
        assert!(!p.has_more(5, i64::MAX));
    }

    #[test]
    fn limit_constants_have_expected_values() {
        assert_eq!(DEFAULT_LIMIT, 20);
        assert_eq!(MAX_LIMIT, 100);
    }
}
