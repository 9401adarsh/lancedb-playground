use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;

/// A writer that duplicates output to both stdout and a log file.
pub struct TeeWriter {
    pub file: File,
}

impl TeeWriter {
    pub fn new(file: File) -> Self {
        Self { file }
    }
}

impl Write for TeeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write_all(buf)?;
        io::stdout().write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        io::stdout().flush()
    }
}

/// Global tee writer wrapped in a Mutex for thread-safe access from the `log_println!` macro.
pub static TEE_WRITER: std::sync::OnceLock<Mutex<TeeWriter>> = std::sync::OnceLock::new();

/// Generates a timestamped log file path like `logs/<mode>_<timestamp>.log`
/// and initializes the global `TeeWriter`.
pub fn init_log(mode_name: &str) -> io::Result<PathBuf> {
    let log_dir = PathBuf::from("./logs");
    fs::create_dir_all(&log_dir)?;

    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!("{}_{}.log", mode_name, timestamp);
    let log_path = log_dir.join(&filename);

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;

    let tee = TeeWriter::new(file);
    TEE_WRITER
        .set(Mutex::new(tee))
        .map_err(|_| io::Error::new(io::ErrorKind::AlreadyExists, "Logger already initialized"))?;

    Ok(log_path)
}

/// Macro that prints to both stdout and the log file.
/// Usage is identical to `println!`.
#[macro_export]
macro_rules! log_println {
    ($($arg:tt)*) => {{
        let msg = format!($($arg)*);
        println!("{}", msg);
        if let Some(writer) = $crate::log::TEE_WRITER.get() {
            if let Ok(mut w) = writer.lock() {
                use std::io::Write;
                let _ = writeln!(w.file, "{}", msg);
            }
        }
    }};
}
