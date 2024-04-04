use std::sync::Arc;
use std::time::Duration;

use common::types::{DetailsLevel, TelemetryDetail};
use segment::common::anonymize::Anonymize;
use storage::rbac::Access;
use tokio::sync::Mutex;

use super::telemetry::TelemetryCollector;

const DETAIL: TelemetryDetail = TelemetryDetail {
    level: DetailsLevel::Level2,
    histograms: false,
};
const REPORTING_INTERVAL: Duration = Duration::from_secs(60 * 60); // One hour

pub struct TelemetryReporter {
    telemetry_url: String,
    telemetry: Arc<Mutex<TelemetryCollector>>,
}

const FULL_ACCESS: Access = Access::full("Telemetry reporter");

impl TelemetryReporter {
    fn new(telemetry: Arc<Mutex<TelemetryCollector>>) -> Self {
        let telemetry_url = if cfg!(debug_assertions) {
            "https://staging-telemetry.qdrant.io".to_string()
        } else {
            "https://telemetry.qdrant.io".to_string()
        };

        Self {
            telemetry_url,
            telemetry,
        }
    }

    async fn report(&self) {
        let data = self
            .telemetry
            .lock()
            .await
            .prepare_data(&FULL_ACCESS, DETAIL)
            .await
            .anonymize();
        let client = reqwest::Client::new();
        let data = serde_json::to_string(&data).unwrap();
        let _resp = client
            .post(&self.telemetry_url)
            .body(data)
            .header("Content-Type", "application/json")
            .send()
            .await;
    }

    pub async fn run(telemetry: Arc<Mutex<TelemetryCollector>>) {
        let reporter = Self::new(telemetry);
        loop {
            reporter.report().await;
            tokio::time::sleep(REPORTING_INTERVAL).await;
        }
    }
}
