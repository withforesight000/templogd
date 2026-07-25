use uuid::Uuid;

/// Creates a new trace identifier in the 32-character lowercase hexadecimal format.
pub(crate) fn new_trace_id() -> String {
    Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::new_trace_id;

    #[test]
    fn creates_lowercase_32_character_trace_ids() {
        let trace_id = new_trace_id();

        assert_eq!(trace_id.len(), 32);
        assert!(trace_id.chars().all(|character| character.is_ascii_hexdigit()));
        assert_eq!(trace_id, trace_id.to_ascii_lowercase());
    }

    #[test]
    fn creates_distinct_trace_ids_for_separate_requests() {
        assert_ne!(new_trace_id(), new_trace_id());
    }
}
