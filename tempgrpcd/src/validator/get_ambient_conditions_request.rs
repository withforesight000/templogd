use garde::Validate;
use pbjson_types::Timestamp;
use tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest;

use crate::validator::error::ValidationError;

/// Borrowed view of `GetAmbientConditionsRequest` with the fields needed for validation.
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
    pub fn validate_business_rules(&self) -> Result<(), ValidationError> {
        let start = self.start_time.as_ref().ok_or_else(|| ValidationError::invalid("start_time is required"))?;
        let end = self.end_time.as_ref().ok_or_else(|| ValidationError::invalid("end_time is required"))?;

        if (start.seconds, start.nanos) > (end.seconds, end.nanos) {
            return Err(ValidationError::invalid("start_time must be <= end_time"));
        }

        if let Some(samples) = self.samples
            && samples == 0
        {
            return Err(ValidationError::invalid("samples must be > 0"));
        }

        Ok(())
    }
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
        assert!(validated.validate_business_rules().is_ok());
    }

    #[test]
    fn validate_business_rules_rejects_missing_start_time() {
        let req = request(None, Some(Timestamp { seconds: 20, nanos: 0 }), Some(3));

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        assert!(validated.validate().is_err());
        let err = validated.validate_business_rules().unwrap_err();
        assert!(matches!(err, ValidationError::Invalid(message) if message == "start_time is required"));
    }

    #[test]
    fn validate_business_rules_rejects_missing_end_time() {
        let req = request(Some(Timestamp { seconds: 10, nanos: 0 }), None, Some(3));

        let validated = ValidatedGetAmbientConditionsRequest::from(&req);

        assert!(validated.validate().is_err());
        let err = validated.validate_business_rules().unwrap_err();
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

        let err = validated.validate_business_rules().unwrap_err();
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

        let err = validated.validate_business_rules().unwrap_err();
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

        let err = validated.validate_business_rules().unwrap_err();
        assert!(matches!(err, ValidationError::Invalid(message) if message == "samples must be > 0"));
    }
}
