//! Local diagnostic subscriber. Raw exchange history includes private account data; this is not
//! a public market-data feed. Run under the exchange's trusted OS account.
use std::{
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};

use super::event_stream::{ReaderCheckpoint, StreamReader};

pub fn run(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() < 2 || args.len() > 4 {
        return Err("usage: stock --event-probe JOURNAL STREAM [CHECKPOINT_JSON] [--once]".into());
    }
    let extra = &args[2..];
    let once = extra.last().is_some_and(|arg| arg == "--once");
    let rest = &extra[..extra.len() - usize::from(once)];
    if rest.len() > 1 || rest.first().is_some_and(|s| s.starts_with("--")) {
        return Err("usage: stock --event-probe JOURNAL STREAM [CHECKPOINT_JSON] [--once]".into());
    }
    let checkpoint_path = rest.first().map(PathBuf::from);
    // Reject destinations that could overwrite live exchange data, including existing symlinks.
    if let Some(path) = &checkpoint_path {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        let absolute = if path.exists() {
            path.canonicalize()?
        } else {
            parent
                .canonicalize()?
                .join(path.file_name().ok_or("invalid checkpoint path")?)
        };
        if absolute == std::fs::canonicalize(&args[0])?
            || absolute == std::fs::canonicalize(&args[1])?
        {
            return Err("checkpoint must not overwrite the journal or stream".into());
        }
    }
    let checkpoint: Option<ReaderCheckpoint> = match &checkpoint_path {
        Some(path) => match std::fs::read(path) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        },
        None => None,
    };
    let mut reader = StreamReader::open(&args[0], &args[1], checkpoint)?;
    let mut output = io::stdout().lock();
    loop {
        match reader.next_batch()? {
            Some(batch) => {
                serde_json::to_writer(&mut output, &batch)?;
                writeln!(output)?;
                output.flush()?;
                if let Some(path) = &checkpoint_path {
                    save_checkpoint(path, &reader.checkpoint())?;
                }
            }
            None if once => return Ok(()),
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

fn save_checkpoint(path: &std::path::Path, checkpoint: &ReaderCheckpoint) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        serde_json::to_writer(&mut file, checkpoint)?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        std::fs::File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}
