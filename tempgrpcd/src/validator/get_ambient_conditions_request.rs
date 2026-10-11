use garde::Validate;
use pbjson_types::Timestamp;
use tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest;
use tracing::instrument;

use crate::validator::error::ValidationError;

const PROTOBUF_MAX_SECONDS: i64 = 253_402_300_799;
const MAX_REDIS_SEQUENCE: u64 = u64::MAX;

/// Inclusive Redis Stream IDs derived from a validated protobuf request.
#[derive(Debug, Eq, PartialEq)]
pub struct RedisTimeRange {
    pub start: String,
    pub end: String,
    pub used_legacy_encoding: bool,
}

/// Borrows only the request fields needed for validation before dispatch.
#[derive(Debug, Validate)]
pub struct ValidatedGetAmbientConditionsRequest<'a> {
    #[garde(required)]
    start_time: Option<&'a Timestamp>,

    #[garde(required)]
    end_time: Option<&'a Timestamp>,

    #[garde(skip)]
    samples: Option<u64>,
}

impl<'a> From<&'a GetAmbientConditionsRequest> for ValidatedGetAmbientConditionsRequest<'a> {
    fn from(req: &'a GetAmbientConditionsRequest) -> Self {
        Self {
            start_time: req.start_time.as_ref(),
            end_time: req.end_time.as_ref(),
            samples: req.samples,
        }
    }
}

impl<'a> ValidatedGetAmbientConditionsRequest<'a> {
    /// Enforce request-specific business rules that are not covered by `garde`.
    ///
    /// This checks that both timestamps exist, `start_time` is not after `end_time`
    /// including nanoseconds, and `samples`, when provided, is greater than zero.
    #[instrument(level = "info", name = "validator.validate_business_rules", skip_all, err)]
    pub fn validate_business_rules(&self, max_samples: u64) -> Result<(), ValidationError> {
        let start = self.start_time.as_ref().ok_or_else(|| ValidationError::invalid("start_time is required"))?;
        let end = self.end_time.as_ref().ok_or_else(|| ValidationError::invalid("end_time is required"))?;

        if let Some(samples) = self.samples
            && (samples == 0 || samples > max_samples)
        {
            return Err(ValidationError::invalid(format!(
                "samples must be between 1 and {max_samples}"
            )));
        }

        validate_nanos(start)?;
        validate_nanos(end)?;
        if (start.seconds, start.nanos) > (end.seconds, end.nanos) {
            return Err(ValidationError::invalid("start_time must be <= end_time"));
        }

        Ok(())
    }

    /// Converts canonical protobuf timestamps, or the explicitly enabled legacy encoding,
    /// into inclusive Redis Stream IDs.
    pub fn to_redis_time_range(&self, accept_legacy: bool) -> Result<RedisTimeRange, ValidationError> {
        let start = self.start_time.ok_or_else(|| ValidationError::invalid("start_time is required"))?;
        let end = self.end_time.ok_or_else(|| ValidationError::invalid("end_time is required"))?;
        let start_legacy = is_legacy(start);
        let end_legacy = is_legacy(end);
        if start_legacy != end_legacy {
            return Err(ValidationError::invalid(
                "start_time and end_time must use the same timestamp encoding",
            ));
        }
        if start_legacy && !accept_legacy {
            return Err(ValidationError::invalid(
                "legacy millisecond timestamp encoding is not accepted",
            ));
        }
        if (start.seconds, start.nanos) > (end.seconds, end.nanos) {
            return Err(ValidationError::invalid("start_time must be <= end_time"));
        }

        let start_ms = timestamp_milliseconds(start, start_legacy, true)?;
        let end_ms = timestamp_milliseconds(end, end_legacy, false)?;
        if start_ms > end_ms {
            return Err(ValidationError::invalid("start_time must be <= end_time"));
        }

        Ok(RedisTimeRange {
            start: format!("{start_ms}-0"),
            end: format!("{end_ms}-{MAX_REDIS_SEQUENCE}"),
            used_legacy_encoding: start_legacy,
        })
    }
}

fn validate_nanos(timestamp: &Timestamp) -> Result<(), ValidationError> {
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(ValidationError::invalid(
            "timestamp nanos must be between 0 and 999999999",
        ));
    }
    Ok(())
}

fn is_legacy(timestamp: &Timestamp) -> bool {
    timestamp.seconds > PROTOBUF_MAX_SECONDS && timestamp.nanos == 0
}

fn timestamp_milliseconds(
    timestamp: &Timestamp,
    legacy: bool,
    round_up_submillisecond: bool,
) -> Result<i64, ValidationError> {
    if legacy {
        return Ok(timestamp.seconds);
    }
    if !(0..=PROTOBUF_MAX_SECONDS).contains(&timestamp.seconds) {
        return Err(ValidationError::invalid(
            "timestamp seconds are outside the supported range",
        ));
    }
    let milliseconds = timestamp
        .seconds
        .checked_mul(1000)
        .and_then(|value| value.checked_add(i64::from(timestamp.nanos) / 1_000_000))
        .ok_or_else(|| ValidationError::invalid("timestamp cannot be represented as milliseconds"))?;
    if round_up_submillisecond && timestamp.nanos % 1_000_000 != 0 {
        return milliseconds
            .checked_add(1)
            .ok_or_else(|| ValidationError::invalid("timestamp cannot be represented as milliseconds"));
    }
    Ok(milliseconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        samples: Option<u64>,
    ) -> GetAmbientConditionsRequest {
        GetAmbientConditionsRequest {
            start_time,
            end_time,
            samples,
        }
    }

    #[test]
    fn from_keeps_borrowed_fields() {
        let req = request(
            Some(Timestamp { seconds: 10, nanos: 0 }),
            Some(Timestamp { seconds: 20, nanos: 0 }),
            Some(3),
        );

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        assert_eq!(validated.start_time.unwrap().seconds, 10);
        assert_eq!(validated.end_time.unwrap().seconds, 20);
        assert_eq!(validated.samples, Some(3));
    }

    #[test]
    fn validate_business_rules_accepts_valid_request() {
        let req = request(
            Some(Timestamp { seconds: 10, nanos: 0 }),
            Some(Timestamp { seconds: 20, nanos: 0 }),
            Some(3),
        );

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        assert!(validated.validate().is_ok());
        assert!(validated.validate_business_rules(1000).is_ok());
        assert!(validated.to_redis_time_range(false).is_ok());
    }

    #[test]
    fn validate_business_rules_rejects_missing_start_time() {
        let req = request(None, Some(Timestamp { seconds: 20, nanos: 0 }), Some(3));

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        assert!(validated.validate().is_err());
        let err = validated.validate_business_rules(1000).unwrap_err();
        assert!(matches!(err, ValidationError::Invalid(message) if message == "start_time is required"));
    }

    #[test]
    fn validate_business_rules_rejects_missing_end_time() {
        let req = request(Some(Timestamp { seconds: 10, nanos: 0 }), None, Some(3));

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        assert!(validated.validate().is_err());
        let err = validated.validate_business_rules(1000).unwrap_err();
        assert!(matches!(err, ValidationError::Invalid(message) if message == "end_time is required"));
    }

    #[test]
    fn validate_business_rules_rejects_reversed_time_range() {
        let req = request(
            Some(Timestamp { seconds: 20, nanos: 0 }),
            Some(Timestamp { seconds: 10, nanos: 0 }),
            Some(3),
        );

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        let err = validated.validate_business_rules(1000).unwrap_err();
        assert!(matches!(err, ValidationError::Invalid(message) if message == "start_time must be <= end_time"));
    }

    #[test]
    fn validate_business_rules_rejects_reversed_nanosecond_range() {
        let req = request(
            Some(Timestamp {
                seconds: 20,
                nanos: 500_000_000,
            }),
            Some(Timestamp {
                seconds: 20,
                nanos: 499_999_999,
            }),
            Some(3),
        );

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        let err = validated.validate_business_rules(1000).unwrap_err();
        assert!(matches!(err, ValidationError::Invalid(message) if message == "start_time must be <= end_time"));
    }

    #[test]
    fn validate_business_rules_rejects_zero_samples() {
        let req = request(
            Some(Timestamp { seconds: 10, nanos: 0 }),
            Some(Timestamp { seconds: 20, nanos: 0 }),
            Some(0),
        );

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        let err = validated.validate_business_rules(1000).unwrap_err();
        assert!(matches!(err, ValidationError::Invalid(message) if message == "samples must be between 1 and 1000"));
    }

    #[test]
    fn rejects_samples_above_the_configured_limit() {
        let req = request(
            Some(Timestamp { seconds: 10, nanos: 0 }),
            Some(Timestamp { seconds: 20, nanos: 0 }),
            Some(1001),
        );

        let err = ValidatedGetAmbientConditionsRequest::from(&req).validate_business_rules(1000).unwrap_err();

        assert!(matches!(err, ValidationError::Invalid(message) if message == "samples must be between 1 and 1000"));
    }

    #[test]
    fn converts_canonical_timestamps_to_inclusive_redis_ids() {
        let req = request(
            Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 123_000_000,
            }),
            Some(Timestamp {
                seconds: 1_700_000_001,
                nanos: 456_000_000,
            }),
            None,
        );

        let range = ValidatedGetAmbientConditionsRequest::from(&req).to_redis_time_range(false).unwrap();

        assert_eq!(range.start, "1700000000123-0");
        assert_eq!(range.end, "1700000001456-18446744073709551615");
        assert!(!range.used_legacy_encoding);
    }

    #[test]
    fn rounds_sub_millisecond_range_boundaries_inward() {
        let req = request(
            Some(Timestamp {
                seconds: 1,
                nanos: 123_500_000,
            }),
            Some(Timestamp {
                seconds: 2,
                nanos: 456_500_000,
            }),
            None,
        );

        let range = ValidatedGetAmbientConditionsRequest::from(&req).to_redis_time_range(false).unwrap();

        assert_eq!(range.start, "1124-0");
        assert_eq!(range.end, "2456-18446744073709551615");
    }

    #[test]
    fn accepts_legacy_milliseconds_only_when_enabled() {
        let req = request(
            Some(Timestamp {
                seconds: 1_700_000_000_123,
                nanos: 0,
            }),
            Some(Timestamp {
                seconds: 1_700_000_001_456,
                nanos: 0,
            }),
            None,
        );
        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        assert!(validated.to_redis_time_range(false).is_err());
        let range = validated.to_redis_time_range(true).unwrap();
        assert_eq!(range.start, "1700000000123-0");
        assert_eq!(range.end, "1700000001456-18446744073709551615");
        assert!(range.used_legacy_encoding);
    }

    #[test]
    fn rejects_mixed_timestamp_encodings_and_invalid_nanos() {
        let mixed = request(
            Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            Some(Timestamp {
                seconds: 1_700_000_001_000,
                nanos: 0,
            }),
            None,
        );
        assert!(ValidatedGetAmbientConditionsRequest::from(&mixed).to_redis_time_range(true).is_err());

        let invalid = request(
            Some(Timestamp {
                seconds: 1,
                nanos: 1_000_000_000,
            }),
            Some(Timestamp { seconds: 2, nanos: 0 }),
            None,
        );
        assert!(ValidatedGetAmbientConditionsRequest::from(&invalid).validate_business_rules(1000).is_err());
    }

    #[test]
    fn rejects_reversed_sub_millisecond_range_before_truncation() {
        let req = request(
            Some(Timestamp {
                seconds: 10,
                nanos: 999_999,
            }),
            Some(Timestamp { seconds: 10, nanos: 0 }),
            None,
        );

        let err = ValidatedGetAmbientConditionsRequest::from(&req).to_redis_time_range(false).unwrap_err();

        assert!(matches!(err, ValidationError::Invalid(message) if message == "start_time must be <= end_time"));
    }
}
