#![cfg(feature = "serde")]
use tracing_forest::{traits::*, util::*, Printer, Tag};
use tracing_subscriber::Registry;

#[test]
#[ignore]
fn test_manual_with_json() {
    let processor = Printer::new().formatter(serde_json::to_string_pretty);
    let layer = ForestLayer::from(processor);
    let subscriber = Registry::default().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        info!("hello, world!");
        info_span!("my-span", answer = 42).in_scope(|| {
            info!("wassup");
        })
    });
}

fn pretty_tag(event: &Event) -> Option<Tag> {
    let level = *event.metadata().level();
    let target = event.metadata().target();

    Some(match target {
        "security" if level == Level::ERROR => Tag::builder()
            .icon('🔐')
            .prefix(target)
            .suffix("critical")
            .build(),
        "security" if level == Level::INFO => Tag::builder()
            .icon('🔓')
            .prefix(target)
            .suffix("access")
            .build(),
        "admin" | "request" | "filter" => Tag::builder().prefix(target).level(level).build(),
        _ => return None,
    })
}

#[test]
fn pretty_example() {
    let _guard = tracing::subscriber::set_default({
        let layer = ForestLayer::new(Printer::new(), pretty_tag);

        Registry::default().with(layer)
    });

    info_span!("try_from_entry_ro").in_scope(|| {
        info_span!("server::internal_search").in_scope(|| {
            info!(target: "filter", "Some filter info...");
            info_span!("server::search").in_scope(|| {
                info_span!("be::search").in_scope(|| {
                    info_span!("be::search -> filter2idl", term="bobby", verbose=false).in_scope(|| {
                        info_span!("be::idl_arc_sqlite::get_idl").in_scope(|| {
                            info!(target: "filter", "Some filter info...");
                        });
                        info_span!("be::idl_arc_sqlite::get_idl").in_scope(|| {
                            error!(target: "admin", "On no, an admin error occurred :(");
                            debug!("An untagged debug log");
                            info!(target: "admin", alive = false, status = "sad", "there's been a big mistake")
                        });
                    });
                });
                info_span!("be::idl_arc_sqlite::get_identry").in_scope(|| {
                    error!(target: "security", "A security critical log");
                    info!(target: "security", "A security access log");
                });
            });
            info_span!("server::search<filter_resolve>").in_scope(|| {
                warn!(target: "filter", "Some filter warning");
            });

        });
        trace!("Finished!");
    });
}
