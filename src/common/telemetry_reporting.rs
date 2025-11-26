use std::sync::Arc;
use std::time::Duration;

use common::defaults::APP_USER_AGENT;
use common::types::{DetailsLevel, TelemetryDetail};
use reqwest::Client;
use segment::common::anonymize::Anonymize;
use storage::content_manager::errors::StorageResult;
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

    async fn report(&self, client: &Client) -> StorageResult<()> {
        let data = self
            .telemetry
            .lock()
            .await
            .prepare_data(&FULL_ACCESS, DETAIL, None)
            .await?
            .anonymize();
        let data = serde_json::to_string(&data)?;
        let resp = client
            .post(&self.telemetry_url)
            .body(data)
            .header("Content-Type", "application/json")
            .send()
            .await?;
        if !resp.status().is_success() {
            log::error!(
                "Failed to report telemetry: resp status:{:?} resp body:{:?}",
                resp.status(),
                resp.text().await?
            );
        }
        Ok(())
    }

    pub async fn run(telemetry: Arc<Mutex<TelemetryCollector>>) {
        let reporter = Self::new(telemetry);
        let client = Client::builder()
            .user_agent(APP_USER_AGENT.as_str())
            .build()
            .unwrap();
        loop {
            if let Err(err) = reporter.report(&client).await {
                log::error!("Failed to report telemetry {err}")
            }
            tokio::time::sleep(REPORTING_INTERVAL).await;
        }
    }
}
