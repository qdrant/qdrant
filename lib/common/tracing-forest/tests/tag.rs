#![cfg(feature = "tokio")]
use tracing_forest::{util::*, Tag};

fn kanidm_tag(event: &Event) -> Option<Tag> {
    let target = event.metadata().target();
    let level = *event.metadata().level();

    Some(match target {
        "security" if level == Level::ERROR => Tag::builder()
            .prefix(target)
            .suffix("critical")
            .icon('🔐')
            .build(),
        "admin" | "request" => Tag::builder().prefix(target).level(level).build(),
        _ => return None,
    })
}

#[tokio::test]
async fn test_kanidm_tag() -> Result<(), Box<dyn std::error::Error>> {
    let logs = tracing_forest::capture()
        .set_tag(kanidm_tag)
        .build()
        .on(async {
            info!(target: "admin", "some info for the admin");
            error!(target: "request", "the request timed out");
            error!(target: "security", "the db has been breached");
            info!("no tags here");
            info!(target: "unrecognized", "unrecognizable tag");
        })
        .await;

    assert!(logs.len() == 5);

    let admin_info = logs[0].event()?;
    assert!(admin_info.message() == Some("some info for the admin"));
    assert!(admin_info.tag().unwrap().to_string() == "admin.info");

    let request_error = logs[1].event()?;
    assert!(request_error.message() == Some("the request timed out"));
    assert!(request_error.tag().unwrap().to_string() == "request.error");

    let security_critical = logs[2].event()?;
    assert!(security_critical.message() == Some("the db has been breached"));
    assert!(security_critical.tag().unwrap().to_string() == "security.critical");

    let no_tags = logs[3].event()?;
    assert!(no_tags.message() == Some("no tags here"));
    assert!(no_tags.tag().is_none());

    let unrecognized = logs[4].event()?;
    assert!(unrecognized.message() == Some("unrecognizable tag"));
    assert!(unrecognized.tag().is_none());

    Ok(())
}
