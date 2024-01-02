#![cfg(feature = "tokio")]
use std::error::Error;
use tokio::time::Duration;
use tracing_forest::{traits::*, util::*};

#[tokio::test]
async fn test_filtering() -> Result<(), Box<dyn Error>> {
    let logs = tracing_forest::capture()
        .build_on(|subscriber| subscriber.with(LevelFilter::INFO))
        .on(async {
            trace!("unimportant information");
            info!("important information");
        })
        .await;

    assert!(logs.len() == 1);

    let info = logs[0].event()?;

    assert!(info.message() == Some("important information"));

    Ok(())
}

#[tokio::test]
async fn duration_checked_sub() -> Result<(), Box<dyn Error>> {
    let logs = tracing_forest::capture()
        .build()
        .on(async {
            let parent = info_span!("parent");
            info_span!(parent: &parent, "child").in_scope(|| {
                // cursed blocking in async lol
                std::thread::sleep(Duration::from_millis(100));
            });
        })
        .await;

    assert!(logs.len() == 1);

    let parent = logs[0].span()?;
    assert!(parent.total_duration() >= parent.inner_duration());

    Ok(())
}

#[tokio::test]
async fn try_get_wrong_tree_variant() -> Result<(), Box<dyn Error>> {
    let logs = tracing_forest::capture()
        .build()
        .on(async {
            info!("This is an event");
            info_span!("This is a span").in_scope(|| {
                info!("hello again");
            });
        })
        .await;

    assert!(logs.len() == 2);

    assert!(logs[0].span().is_err());
    assert!(logs[0].event().is_ok());

    assert!(logs[1].event().is_err());
    let span = logs[1].span()?;

    assert!(span.nodes().len() == 1);
    let node = &span.nodes()[0];

    assert!(node.span().is_err());
    assert!(node.event().is_ok());

    Ok(())
}
