#[derive(Debug)]
pub struct AmbientCondition {
    temperature: f64,
    humidity: f64,
    illumination: f64,
}

pub fn new(temperature: f64, humidity: f64, illumination: f64) -> AmbientCondition {
    AmbientCondition {
        temperature,
        humidity,
        illumination,
    }
}

impl AmbientCondition {
    pub fn get_temperature(&self) -> f64 {
        self.temperature
    }

    pub fn get_humidity(&self) -> f64 {
        self.humidity
    }

    pub fn get_illumination(&self) -> f64 {
        self.illumination
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getters_return_values() {
        let cond = new(1.1, 2.2, 3.3);
        assert!((cond.get_temperature() - 1.1).abs() < f64::EPSILON);
        assert!((cond.get_humidity() - 2.2).abs() < f64::EPSILON);
        assert!((cond.get_illumination() - 3.3).abs() < f64::EPSILON);
    }
}
