use byteorder::{BigEndian, ByteOrder};
use std::convert::From;
use std::io::{self, ErrorKind};
use std::io::{Read, Write};
// use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{error, info};

// Buffers for the channels
const RESULTS_BUFFER: usize = 32;
const HEADERS_BUFFER: usize = 32;
const ENTRIES_BUFFER: usize = 128;
const ENTRY_RSP_BUFFER: usize = 32;
const HEADER_SIZE: usize = 38;
const FIXED_SIZE_FILE_ENTRY: usize = 17;

// Entry type for a data file entry
#[derive(Debug, Default)]
pub struct Entry {
    packet_type: u8,       // 2:Data entry, 0:Padding
    length: u32,           // Total length of the entry (17 bytes + length(data))
    entry_type: EntryType, // 0xb0:Bookmark, 1:Event1, 2:Event2,...
    number: u64,           // Entry number (sequential starting with 0)
    data: Vec<u8>,
}

// HeaderEntry type for a header entry
#[derive(Debug, Default)]
pub struct HeaderEntry {
    packet_type: u8,         // 1:Header
    head_length: u32,        // Total length of header entry (38)
    version: u8,             // Stream file version
    system_id: u64,          // System identifier (e.g. ChainID)
    stream_type: StreamType, // 1:Sequencer
    total_length: u64,       // Total bytes used in the file
    total_entries: u64,      // Total number of data entries (packet type PtData)
}

// ResultEntry type for a result entry
#[derive(Debug, Default)]
pub struct ResultEntry {
    packet_type: u8, // 0xff:Result
    length: u32,
    error_num: u32, // 0:No error
    error_str: Vec<u8>,
}

// EntryType enum represents the entry event types
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum EntryType {
    #[default]
    NotFound = 0, // EntryTypeNotFound for entry not found
    Bookmark = 0xb0, // EntryTypeBookmark for bookmark entry
    Event1 = 1,      // EntryTypeEvent1 for event type 1
    Event2 = 2,      // EntryTypeEvent2 for event type 2
}

impl From<u32> for EntryType {
    fn from(v: u32) -> Self {
        match v {
            0 => EntryType::NotFound,
            0xb0 => EntryType::Bookmark,
            1 => EntryType::Event1,
            2 => EntryType::Event2,
            _ => EntryType::NotFound,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Command {
    CmdStart = 1,     // CmdStart for the start from entry TCP client command
    CmdStop,          // CmdStop for the stop TCP client command
    CmdHeader,        // CmdHeader for the header TCP client command
    CmdStartBookmark, // CmdStartBookmark for the start from bookmark TCP client command
    CmdEntry,         // CmdEntry for the get entry TCP client command
    CmdBookmark,      // CmdBookmark for the get bookmark TCP client command
}

#[derive(Debug, Clone, Copy)]
pub enum CommandError {
    CmdErrOK = 0,             // CmdErrOK for no error
    CmdErrAlreadyStarted,     // CmdErrAlreadyStarted for client already started error
    CmdErrAlreadyStopped,     // CmdErrAlreadyStopped for client already stopped error
    CmdErrBadFromEntry,       // CmdErrBadFromEntry for invalid starting entry number
    CmdErrBadFromBookmark,    // CmdErrBadFromBookmark for invalid starting bookmark
    CmdErrInvalidCommand = 9, // CmdErrInvalidCommand for invalid/unknown command error
}

// StreamType enum represents the stream types
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum StreamType {
    #[default]
    Sequencer = 1, // Sequencer for sequencer stream type
}

impl From<u64> for StreamType {
    fn from(v: u64) -> Self {
        match v {
            1 => StreamType::Sequencer,
            _ => StreamType::Sequencer,
        }
    }
}

// PacketType enum represents the packet types
#[derive(Debug, Clone, Copy)]
pub enum PacketType {
    PtPadding = 0,    // PtPadding is packet type for pad
    PtHeader = 1,     // PtHeader is packet type just for the header page
    PtData = 2,       // PtData is packet type for data entry
    PtDataRsp = 0xfe, // PtDataRsp is packet type for command response with data
    PtResult = 0xff, // PtResult is packet type not stored/present in file (just for client command result)
}

// Type of the callback function to process the received entry
type ProcessEntryFunc = fn(Entry) -> Result<(), ClientError>;

// ClientError enum represents the client errors
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("{0} Client not started")]
    ClientNotStarted(&'static str),
    #[error("Error executing command: {0}")]
    InvalidCommand(&'static str),
    #[error("Error network")]
    NetworkError(std::io::Error),
    #[error("Errors entry not found")]
    EntryNotFound,
    #[error("Error bookmark not found")]
    BookmarkNotFound,
}

#[derive(Debug)]
// StreamClient type to manage a data stream client
pub struct StreamClient {
    server: String, // Server address to connect IP:port
    stream_type: StreamType,
    conn: Option<TcpStream>,
    id: String,         // Client id
    started: bool,      // Flag client started
    connected: bool,    // Flag client connected to server
    streaming: bool,    // Flag client streaming started
    from_stream: u64,   // Start entry number from latest start command
    total_entries: u64, // Total entries from latest header command

    next_entry: u64,                 // Next entry number to receive from streaming
    process_entry: ProcessEntryFunc, // Callback function to process the entry
}

impl StreamClient {
    pub fn new(server: String) -> Result<StreamClient, Box<dyn std::error::Error>> {
        let client = StreamClient {
            server: server.clone(),
            stream_type: StreamType::Sequencer,
            conn: None,
            id: String::new(),
            started: false,
            connected: false,
            streaming: false,
            from_stream: 0,
            total_entries: 0,
            next_entry: 0,

            process_entry: print_received_entry,
        };

        Ok(client)
    }

    // Start connects to the data stream server and starts getting data from the server
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Connect to server
        self.connect_server()?;
        // self.exec_command_start(0)?;
        self.read_entries().await;

        Ok(())
    }

    // connect_server waits until the server connection is established and returns if a command result is pending
    fn connect_server(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        // Connect to server
        while !self.connected {
            match TcpStream::connect(&self.server) {
                Ok(conn) => {
                    // Connected
                    self.conn = Some(conn);
                    self.connected = true;
                    self.id = self.conn.as_ref().unwrap().local_addr()?.to_string();
                    info!("{} Connected to server: {}", self.id, self.server);

                    // Restore streaming
                    if self.streaming {
                        match self.exec_command(Command::CmdStart, 0, None) {
                            Ok(_) => {}
                            Err(e) => {
                                self.close_connection();
                                thread::sleep(Duration::from_secs(5));
                                info!("Error restoring streaming: {:?}", e);
                                self.streaming = false;
                                continue;
                            }
                        }
                        return Ok(true);
                    } else {
                        return Ok(false);
                    }
                }
                Err(e) => {
                    error!("Error connecting to server {}: {}", self.server, e);
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            }
        }
        Ok(false)
    }

    async fn read_entries(&mut self) {
        let mut packet = [0u8; 1];

        // Get the command result
        let mut conn = self.conn.as_ref().unwrap();

        let ip = conn.peer_addr().unwrap().ip().to_string();
        conn.read_exact(&mut packet).expect("Error reading packet");

        match packet[0] {
            0 => {
                info!("Received packet type: {:?}", PacketType::PtPadding);
            }
            1 => {
                info!("Received packet type: {:?}", PacketType::PtHeader);
            }
            2 => {
                info!("Received packet type: {:?}", PacketType::PtData);
            }
            0xfe => {
                info!("Received packet type: {:?}", PacketType::PtDataRsp);
            }
            0xff => {
                info!("Received packet type: {:?}", PacketType::PtResult);
            }
            _ => {
                info!("Received packet type: Unknown");
            }
        }

        //     // Create a new channel with a capacity of at most 32.
        //     let (tx, mut rx) = mpsc::unbounded_channel::<Entry>();

        //     // Start receiving messages
        // while let Some(entry) = rx.recv().await {
        //     match entry {

        //     }
        // }
    }

    async fn get_streaming(&self) {
        // Implement the logic to consume streaming entries
    }

    // close_connection closes connection to the server
    pub fn close_connection(&mut self) {
        if self.connected {
            info!("{} Close connection", self.id);
            // self.conn.close(); // Uncomment this when you have a connection to close
        }
        self.connected = false;
    }

    // exec_command_start executes client TCP command to start streaming from entry
    pub fn exec_command_start(&mut self, from_entry: u64) -> Result<(), ClientError> {
        match self.exec_command(Command::CmdStart, from_entry, None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // exec_command_start_bookmark executes client TCP command to start streaming from bookmark
    pub fn exec_command_start_bookmark(
        &mut self,
        from_bookmark: Vec<u8>,
    ) -> Result<(), ClientError> {
        match self.exec_command(Command::CmdStartBookmark, 0, Some(from_bookmark)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // exec_command_stop executes client TCP command to stop streaming
    pub fn exec_command_stop(&mut self) -> Result<(), ClientError> {
        match self.exec_command(Command::CmdStop, 0, None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // exec_command_get_header executes client TCP command to get the header
    pub fn exec_command_get_header(&mut self) -> Result<HeaderEntry, ClientError> {
        match self.exec_command(Command::CmdHeader, 0, None) {
            Ok((header, _)) => Ok(header),
            Err(e) => Err(e),
        }
    }

    // exec_command_get_entry executes client TCP command to get an entry
    pub fn exec_command_get_entry(&mut self, from_entry: u64) -> Result<Entry, ClientError> {
        match self.exec_command(Command::CmdEntry, from_entry, None) {
            Ok((_, entry)) => Ok(entry),
            Err(e) => Err(e),
        }
    }

    // exec_command_get_bookmark executes client TCP command to get a bookmark
    pub fn exec_command_get_bookmark(
        &mut self,
        from_bookmark: Vec<u8>,
    ) -> Result<Entry, ClientError> {
        match self.exec_command(Command::CmdBookmark, 0, Some(from_bookmark)) {
            Ok((_, entry)) => Ok(entry),
            Err(e) => Err(e),
        }
    }

    // exec_command executes a valid client TCP command with deferred command result possibility
    fn exec_command(
        &mut self,
        cmd: Command,
        from_entry: u64,
        from_bookmark: Option<Vec<u8>>,
    ) -> Result<(HeaderEntry, Entry), ClientError> {
        info!("{} Executing command {:?}...", self.id, cmd,);
        let mut header: HeaderEntry = Default::default();
        let mut entry: Entry = Default::default();

        // Check status of the client
        if !self.connected {
            info!("Execute command not allowed. Client is not started");
            return Err(ClientError::ClientNotStarted(
                "Execute command not allowed.",
            ));
        }

        let mut conn = self.conn.as_ref().unwrap();

        // Send command
        conn.write_all(&(cmd as u64).to_le_bytes())
            .expect("Error sending command");

        // Send stream type
        conn.write_all(&(self.stream_type as u64).to_le_bytes())
            .expect("Error sending stream type");

        // Send the command parameters
        match cmd {
            Command::CmdStart => {
                info!("{} ...from entry {}", self.id, from_entry);
                // Send starting/from entry number
                conn.write_all(&from_entry.to_le_bytes())
                    .expect("Error sending Start command");
            }
            Command::CmdStartBookmark => {
                info!("{} ...from bookmark {:?}", self.id, from_bookmark);
                // Send starting/from bookmark length
                if let Some(bookmark) = &from_bookmark {
                    conn.write_all(&(bookmark.len() as u32).to_le_bytes())
                        .expect("Error sending StartBookmark command");
                    // Send starting/from bookmark
                    conn.write_all(bookmark)
                        .expect("Error sending from bookmark");
                }
            }
            Command::CmdEntry => {
                info!("{} ...get entry {}", self.id, from_entry);
                // Send entry to retrieve
                conn.write_all(&from_entry.to_le_bytes())
                    .expect("Error sending entry");
            }
            Command::CmdBookmark => {
                info!("{} ...get bookmark {:?}", self.id, from_bookmark);
                // Send bookmark length
                if let Some(bookmark) = &from_bookmark {
                    conn.write_all(&(bookmark.len() as u32).to_le_bytes())
                        .expect("Error sending bookmark length");
                    // Send bookmark to retrieve
                    conn.write_all(bookmark).expect("Error sending bookmark");
                }
            }
            _ => {}
        }

        // Get the command result
        let mut buf = vec![0u8; ENTRY_RSP_BUFFER];
        conn.read_to_end(&mut buf).expect("Error reading response");

        // Get the data response and update streaming flag
        match cmd {
            Command::CmdStart => {
                self.streaming = true;
                self.from_stream = from_entry;
            }
            Command::CmdStartBookmark => {
                self.streaming = true;
            }
            Command::CmdStop => {
                self.streaming = false;
            }
            Command::CmdHeader => {
                let h = decode_binary_to_header_entry(&mut buf).expect("Error decoding header");
                header = h;
                self.total_entries = header.total_entries;
            }
            Command::CmdEntry => {
                let e = decode_binary_to_file_entry(&mut buf).expect("Error decoding entry");
                if e.entry_type == EntryType::NotFound {
                    return Err(ClientError::EntryNotFound);
                }
                entry = e;
            }
            Command::CmdBookmark => {
                let e = decode_binary_to_file_entry(&mut buf).expect("Error decoding bookmark");
                if e.entry_type == EntryType::NotFound {
                    return Err(ClientError::BookmarkNotFound);
                }
                entry = e;
            }
        }

        Ok((header, entry))
    }
}

// decode_binary_to_header_entry decodes from binary bytes slice to a header entry type
fn decode_binary_to_header_entry(b: &[u8]) -> io::Result<HeaderEntry> {
    if b.len() != HEADER_SIZE {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Invalid binary header entry",
        ));
    }

    let packet_type = b[0];
    let head_length = BigEndian::read_u32(&b[1..5]);
    let version = b[5];
    let system_id = BigEndian::read_u64(&b[6..14]);
    let stream_type = StreamType::from(BigEndian::read_u64(&b[14..22])); // Convert u64 to StreamType
    let total_length = BigEndian::read_u64(&b[22..30]);
    let total_entries = BigEndian::read_u64(&b[30..38]);

    Ok(HeaderEntry {
        packet_type,
        head_length,
        version,
        system_id,
        stream_type,
        total_length,
        total_entries,
    })
}

// decode_binary_to_file_entry decodes from binary bytes slice to file entry type
fn decode_binary_to_file_entry(b: &[u8]) -> io::Result<Entry> {
    if b.len() < FIXED_SIZE_FILE_ENTRY {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Invalid binary data entry",
        ));
    }

    let packet_type = b[0];
    let length = BigEndian::read_u32(&b[1..5]);
    let entry_type = EntryType::from(BigEndian::read_u32(&b[5..9])); // Convert u32 to EntryType
    let number = BigEndian::read_u64(&b[9..17]);
    let data = b[17..].to_vec();

    if data.len() as u32 != length - FIXED_SIZE_FILE_ENTRY as u32 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Error decoding binary data entry",
        ));
    }

    Ok(Entry {
        packet_type,
        length,
        entry_type,
        number,
        data,
    })
}

fn print_received_entry(entry: Entry) -> Result<(), ClientError> {
    info!("Received entry: {:?}", entry);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_stream_client_new() {
        let server = "127.0.0.1:7900".to_string(); // "stream.zkevm-rpc.com:6900".to_string();
        let stream_type = StreamType::Sequencer;
        let mut client = StreamClient::new(server.clone()).unwrap();
        assert_eq!(client.server, server);
        assert_eq!(client.stream_type, stream_type);

        client.start().await.unwrap();
    }
}
