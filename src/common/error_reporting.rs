use std::time::Duration;

pub struct ErrorReporter;

impl ErrorReporter {
    fn get_url() -> String {
        if cfg!(debug_assertions) {
            "https://staging-telemetry.qdrant.io".to_string()
        } else {
            "https://telemetry.qdrant.io".to_string()
        }
    }

    pub fn report(error: &str, reporting_id: &str, backtrace: Option<&str>) {
        let client = reqwest::blocking::Client::new();

        let report = serde_json::json!({
            "id": reporting_id,
            "error": error,
            "backtrace": backtrace.unwrap_or(""),
        });

        let data = serde_json::to_string(&report).unwrap();
        let _resp = client
            .post(Self::get_url())
            .body(data)
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(1))
            .send();
    }
}
