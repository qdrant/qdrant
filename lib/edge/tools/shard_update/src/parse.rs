//! Point id parsing, from the command line and from the interactive prompt.

use anyhow::{Context, Result};
use edge::PointId;

/// Parse a point id from the command line: a bare integer id or a UUID string.
pub fn parse_point_id(raw: &str) -> Result<PointId> {
    // `PointId` deserializes a JSON number into a numeric id; a bare UUID is not
    // valid JSON, so quote it and retry as a JSON string.
    if let Ok(id) = serde_json::from_str::<PointId>(raw) {
        return Ok(id);
    }
    let quoted = serde_json::to_string(raw).expect("string is always serializable");
    serde_json::from_str::<PointId>(&quoted)
        .with_context(|| format!("invalid point id (not an integer or UUID): {raw:?}"))
}

/// Read the next round's ids from stdin: comma-separated on one line, with
/// unparsable input re-prompted; an empty line or EOF ends the session.
pub fn prompt_next_ids() -> Result<Option<Vec<PointId>>> {
    loop {
        eprint!("next ids (comma-separated, empty to quit): ");
        let mut line = String::new();
        if std::io::stdin()
            .read_line(&mut line)
            .context("failed to read ids from stdin")?
            == 0
        {
            return Ok(None);
        }
        let line = line.trim();
        if line.is_empty() {
            return Ok(None);
        }

        match line
            .split(',')
            .map(|raw| parse_point_id(raw.trim()))
            .collect::<Result<Vec<_>>>()
        {
            Ok(ids) => return Ok(Some(ids)),
            Err(err) => log::warn!("{err:#}"),
        }
    }
}
