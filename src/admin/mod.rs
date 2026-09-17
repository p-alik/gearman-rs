//! The Gearman admin text protocol: line-oriented commands on the same TCP
//! port as the binary protocol (the server dispatches on the first byte —
//! `\0` means a binary packet, anything else a text command line). This is
//! a wholly different framing from [`crate::protocol::GearmanCodec`], so
//! [`AdminClient`] talks to the raw stream directly rather than going
//! through [`crate::Connection`].
//!
//! Response framing is *not* uniform across commands: `status`, `workers`,
//! `prioritystatus`, `show jobs`, and `show unique jobs` are multi-line,
//! terminated by a line containing just `.`; every other command replies
//! with exactly one `OK[ <value>]` or `ERR <CODE> <text>` line. This
//! matches `libgearman-server/text.cc`'s `server_run_text`, the reference
//! used to build this module.

use tokio::io::{
    split, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader,
    ReadHalf, WriteHalf,
};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::error::{GearmanError, Result};

pub struct AdminClient<S = TcpStream> {
    reader: BufReader<ReadHalf<S>>,
    writer: WriteHalf<S>,
}

impl AdminClient<TcpStream> {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::from_stream(stream))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AdminClient<S> {
    pub fn from_stream(stream: S) -> Self {
        let (reader, writer) = split(stream);
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    async fn send_line(&mut self, line: &str) -> Result<()> {
        // CRLF, not bare LF: gearmand's `create function` handler computes
        // the function name length as `arg_size - 2`, assuming the line was
        // CRLF-terminated (libgearman-server/text.cc). A bare LF silently
        // truncates the last character of the function name.
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn read_line(&mut self) -> Result<String> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).await?;
        if n == 0 {
            return Err(GearmanError::ConnectionClosed);
        }
        while line.ends_with(['\n', '\r']) {
            line.pop();
        }
        Ok(line)
    }

    async fn read_dot_terminated(&mut self) -> Result<Vec<String>> {
        let mut lines = Vec::new();
        loop {
            let line = self.read_line().await?;
            if line == "." {
                return Ok(lines);
            }
            lines.push(line);
        }
    }

    /// Reads a single `OK[ <value>]` / `ERR <CODE> <text>` reply line.
    async fn read_ack(&mut self) -> Result<Option<String>> {
        let line = self.read_line().await?;
        parse_ack_line(line)
    }

    async fn read_ack_value(&mut self) -> Result<String> {
        self.read_ack()
            .await?
            .ok_or(GearmanError::AdminProtocolError {
                line: "OK".to_string(),
            })
    }

    /// `status`: current job counts per registered function.
    pub async fn status(&mut self) -> Result<Vec<FunctionStatus>> {
        self.send_line("status").await?;
        self.read_dot_terminated()
            .await?
            .iter()
            .map(|line| parse_status_line(line))
            .collect()
    }

    /// `workers`: every attached worker connection and the functions it
    /// registered with `CAN_DO`.
    pub async fn workers(&mut self) -> Result<Vec<WorkerInfo>> {
        self.send_line("workers").await?;
        self.read_dot_terminated()
            .await?
            .iter()
            .map(|line| parse_worker_line(line))
            .collect()
    }

    /// `prioritystatus`: queued job counts per function, broken out by
    /// priority (job assignment priority is global across functions, not
    /// per-function, but these counts are reported per function).
    pub async fn priority_status(
        &mut self,
    ) -> Result<Vec<PriorityFunctionStatus>> {
        self.send_line("prioritystatus").await?;
        self.read_dot_terminated()
            .await?
            .iter()
            .map(|line| parse_priority_status_line(line))
            .collect()
    }

    /// `show jobs`: every currently queued/running job handle.
    pub async fn show_jobs(&mut self) -> Result<Vec<JobListEntry>> {
        self.send_line("show jobs").await?;
        self.read_dot_terminated()
            .await?
            .iter()
            .map(|line| parse_job_list_line(line))
            .collect()
    }

    /// `show unique jobs`: every unique id the server is processing or
    /// queuing.
    pub async fn show_unique_jobs(&mut self) -> Result<Vec<String>> {
        self.send_line("show unique jobs").await?;
        self.read_dot_terminated().await
    }

    /// `cancel job <handle>`.
    pub async fn cancel_job(&mut self, handle: &str) -> Result<()> {
        self.send_line(&format!("cancel job {handle}")).await?;
        self.read_ack().await.map(|_| ())
    }

    /// `create function <name>`.
    pub async fn create_function(&mut self, name: &str) -> Result<()> {
        self.send_line(&format!("create function {name}")).await?;
        self.read_ack().await.map(|_| ())
    }

    /// `drop function <name>`.
    pub async fn drop_function(&mut self, name: &str) -> Result<()> {
        self.send_line(&format!("drop function {name}")).await?;
        self.read_ack().await.map(|_| ())
    }

    /// `maxqueue <function> ...`.
    pub async fn set_max_queue(
        &mut self,
        function: &str,
        size: MaxQueueSize,
    ) -> Result<()> {
        let cmd = match size {
            MaxQueueSize::Default => format!("maxqueue {function}"),
            MaxQueueSize::Uniform(n) => format!("maxqueue {function} {n}"),
            MaxQueueSize::PerPriority { high, normal, low } => {
                format!("maxqueue {function} {high} {normal} {low}")
            }
        };
        self.send_line(&cmd).await?;
        self.read_ack().await.map(|_| ())
    }

    /// `getpid`: the server process id.
    pub async fn getpid(&mut self) -> Result<u32> {
        self.send_line("getpid").await?;
        let value = self.read_ack_value().await?;
        value.parse().map_err(|_| GearmanError::AdminProtocolError {
            line: format!("OK {value}"),
        })
    }

    /// `verbose`: the server's current verbosity level name.
    pub async fn verbose(&mut self) -> Result<String> {
        self.send_line("verbose").await?;
        self.read_ack_value().await
    }

    /// `version`: the server's version string.
    pub async fn version(&mut self) -> Result<String> {
        self.send_line("version").await?;
        self.read_ack_value().await
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MaxQueueSize {
    /// Reset to the server's default queue size for all priorities.
    Default,
    /// Apply the same limit to all priorities.
    Uniform(u32),
    /// Set each priority's queue limit independently.
    PerPriority { high: u32, normal: u32, low: u32 },
}

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub fd: i32,
    pub ip: String,
    pub client_id: String,
    pub functions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FunctionStatus {
    pub function: String,
    pub total: u64,
    pub running: u64,
    pub worker_count: u64,
}

#[derive(Debug, Clone)]
pub struct PriorityFunctionStatus {
    pub function: String,
    pub high: u64,
    pub normal: u64,
    pub low: u64,
    pub worker_count: u64,
}

#[derive(Debug, Clone)]
pub struct JobListEntry {
    pub handle: String,
    pub retries: u32,
    pub ignore_job: bool,
    pub queued: bool,
}

fn parse_ack_line(line: String) -> Result<Option<String>> {
    if let Some(rest) = line.strip_prefix("OK") {
        let rest = rest.trim_start();
        return Ok(if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        });
    }
    if let Some(rest) = line.strip_prefix("ERR") {
        let rest = rest.trim_start();
        let mut parts = rest.splitn(2, ' ');
        let code = parts.next().unwrap_or_default().to_string();
        let text = parts.next().unwrap_or_default().to_string();
        return Err(GearmanError::ServerError { code, text });
    }
    Err(GearmanError::AdminProtocolError { line })
}

fn next_field<'a>(
    fields: &mut std::str::Split<'a, char>,
    line: &str,
) -> Result<&'a str> {
    fields
        .next()
        .ok_or_else(|| GearmanError::AdminProtocolError {
            line: line.to_string(),
        })
}

fn parse_u64(field: &str, line: &str) -> Result<u64> {
    field.parse().map_err(|_| GearmanError::AdminProtocolError {
        line: line.to_string(),
    })
}

fn parse_u32(field: &str, line: &str) -> Result<u32> {
    field.parse().map_err(|_| GearmanError::AdminProtocolError {
        line: line.to_string(),
    })
}

/// `status` line: `FUNCTION\tTOTAL\tRUNNING\tWORKER_COUNT`.
fn parse_status_line(line: &str) -> Result<FunctionStatus> {
    let mut fields = line.split('\t');
    let function = next_field(&mut fields, line)?.to_string();
    let total = parse_u64(next_field(&mut fields, line)?, line)?;
    let running = parse_u64(next_field(&mut fields, line)?, line)?;
    let worker_count = parse_u64(next_field(&mut fields, line)?, line)?;
    Ok(FunctionStatus {
        function,
        total,
        running,
        worker_count,
    })
}

/// `prioritystatus` line: `FUNCTION\tHIGH\tNORMAL\tLOW\tWORKER_COUNT`.
fn parse_priority_status_line(line: &str) -> Result<PriorityFunctionStatus> {
    let mut fields = line.split('\t');
    let function = next_field(&mut fields, line)?.to_string();
    let high = parse_u64(next_field(&mut fields, line)?, line)?;
    let normal = parse_u64(next_field(&mut fields, line)?, line)?;
    let low = parse_u64(next_field(&mut fields, line)?, line)?;
    let worker_count = parse_u64(next_field(&mut fields, line)?, line)?;
    Ok(PriorityFunctionStatus {
        function,
        high,
        normal,
        low,
        worker_count,
    })
}

/// `show jobs` line: `HANDLE\tRETRIES\tIGNORE_JOB\tQUEUED`.
fn parse_job_list_line(line: &str) -> Result<JobListEntry> {
    let mut fields = line.split('\t');
    let handle = next_field(&mut fields, line)?.to_string();
    let retries = parse_u32(next_field(&mut fields, line)?, line)?;
    let ignore_job = next_field(&mut fields, line)? != "0";
    let queued = next_field(&mut fields, line)? != "0";
    Ok(JobListEntry {
        handle,
        retries,
        ignore_job,
        queued,
    })
}

/// `workers` line: `FD IP CLIENT_ID : FUNC1 FUNC2 ...` (functions may be
/// absent). Split on the literal `" :"` rather than the first bare `:`, so
/// an IPv6 address in the IP field doesn't get mistaken for the delimiter.
fn parse_worker_line(line: &str) -> Result<WorkerInfo> {
    let (prefix, functions_part) = line.split_once(" :").ok_or_else(|| {
        GearmanError::AdminProtocolError {
            line: line.to_string(),
        }
    })?;
    let mut fields = prefix.split_whitespace();
    let fd = parse_i32(next_field2(&mut fields, line)?, line)?;
    let ip = next_field2(&mut fields, line)?.to_string();
    let client_id = next_field2(&mut fields, line)?.to_string();
    let functions = functions_part
        .split_whitespace()
        .map(str::to_string)
        .collect();
    Ok(WorkerInfo {
        fd,
        ip,
        client_id,
        functions,
    })
}

fn next_field2<'a>(
    fields: &mut std::str::SplitWhitespace<'a>,
    line: &str,
) -> Result<&'a str> {
    fields
        .next()
        .ok_or_else(|| GearmanError::AdminProtocolError {
            line: line.to_string(),
        })
}

fn parse_i32(field: &str, line: &str) -> Result<i32> {
    field.parse().map_err(|_| GearmanError::AdminProtocolError {
        line: line.to_string(),
    })
}
