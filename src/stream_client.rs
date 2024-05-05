use std::net::TcpStream;
use std::time::Duration;
use std::thread;

// Buffers for the channels
const RESULTS_BUFFER: usize = 32;
const HEADERS_BUFFER: usize = 32;
const ENTRIES_BUFFER: usize = 128;
const ENTRY_RSP_BUFFER: usize = 32;

// Entry type for a data file entry
#[derive(Debug)]
pub struct Entry {
    packet_type: u8,     // 2:Data entry, 0:Padding
    length: u32,    // Total length of the entry (17 bytes + length(data))
    entry_type: EntryType, // 0xb0:Bookmark, 1:Event1, 2:Event2,...
    number: u64,    // Entry number (sequential starting with 0)
    data: Vec<u8>,
}

// HeaderEntry type for a header entry
#[derive(Debug)]
pub struct HeaderEntry {
    packet_type: u8,      // 1:Header
    head_length: u32,     // Total length of header entry (38)
    version: u8,          // Stream file version
    system_id: u64,       // System identifier (e.g. ChainID)
    stream_type: StreamType, // 1:Sequencer
    total_length: u64,     // Total bytes used in the file
    total_entries: u64,     // Total number of data entries (packet type PtData)
}

// EntryType enum represents the entry event types
#[derive(Debug, Clone, Copy)]
pub enum EntryType {
    EntryTypeNotFound = 0, // EntryTypeNotFound for entry not found
    EntryTypeBookmark = 0xb0, // EntryTypeBookmark for bookmark entry
    EntryTypeEvent1 = 1, // EntryTypeEvent1 for event type 1
    EntryTypeEvent2 = 2, // EntryTypeEvent2 for event type 2
}

#[derive(Debug, Clone, Copy)]
pub enum Command {
    CmdStart = 1, // CmdStart for the start from entry TCP client command
    CmdStop, // CmdStop for the stop TCP client command
    CmdHeader, // CmdHeader for the header TCP client command
    CmdStartBookmark, // CmdStartBookmark for the start from bookmark TCP client command
    CmdEntry, // CmdEntry for the get entry TCP client command
    CmdBookmark, // CmdBookmark for the get bookmark TCP client command
}

#[derive(Debug, Clone, Copy)]
pub enum CommandError {
    CmdErrOK = 0, // CmdErrOK for no error
    CmdErrAlreadyStarted, // CmdErrAlreadyStarted for client already started error
    CmdErrAlreadyStopped, // CmdErrAlreadyStopped for client already stopped error
    CmdErrBadFromEntry, // CmdErrBadFromEntry for invalid starting entry number
    CmdErrBadFromBookmark, // CmdErrBadFromBookmark for invalid starting bookmark
    CmdErrInvalidCommand = 9, // CmdErrInvalidCommand for invalid/unknown command error
}

// StreamType enum represents the stream types
#[derive(Debug, Clone, Copy)]
pub enum StreamType {
    Sequencer = 1, // Sequencer for sequencer stream type
}

// Type of the callback function to process the received entry
type ProcessEntryFunc = fn(Entry) -> Result<(), Box<dyn std::error::Error>>;

enum ClientError {
    ClientNotStarted,
    InvalidCommand,
}

#[derive(Debug)]
// StreamClient type to manage a data stream client
pub struct StreamClient {
    server: String, // Server address to connect IP:port
    stream_type: StreamType,
    conn: TcpStream,
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
    pub fn new(
        server: String,
    ) -> Result<StreamClient, Box<dyn std::error::Error>> {
        let client = StreamClient {
            server,
            stream_type: StreamType::Sequencer,
            conn: TcpStream::connect(&server)?,
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
        self.read_entries();

        Ok(())
    }

    // connect_server waits until the server connection is established and returns if a command result is pending
    fn connect_server(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        // Connect to server
        while !self.connected {
            match TcpStream::connect(&self.server) {
                Ok(conn) => {
                    // Connected
                    self.connected = true;
                    self.id = conn.local_addr()?.to_string();
                    println!("{} Connected to server: {}", self.id, self.server);

                    // Restore streaming
                    if self.streaming {
                        match self.exec_command(Command::CmdStart, true, 0, None) {
                            Ok(_) => {}
                            Err(e) => {
                                self.close_connection();
                                thread::sleep(Duration::from_secs(5));
                                println!("Error restoring streaming: {}", e);
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
                    println!("Error connecting to server {}: {}", self.server, e);
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            }
        }
        Ok(false)
    }

    async fn read_entries(&self) {
        // Implement the logic to read entries from the server
    }

    async fn get_streaming(&self) {
        // Implement the logic to consume streaming entries
    }

    // close_connection closes connection to the server
    pub fn close_connection(&mut self) {
        if self.connected {
            println!("{} Close connection", self.id);
            // self.conn.close(); // Uncomment this when you have a connection to close
        }
        self.connected = false;
    }

    // exec_command_start executes client TCP command to start streaming from entry
    pub fn exec_command_start(&self, from_entry: u64) -> Result<(), Box<dyn std::error::Error>> {
        match self.exec_command(Command::CmdStart, false, from_entry, None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // exec_command_start_bookmark executes client TCP command to start streaming from bookmark
    pub fn exec_command_start_bookmark(
        &self,
        from_bookmark: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self.exec_command(Command::CmdStartBookmark, false, 0, Some(from_bookmark)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // exec_command_stop executes client TCP command to stop streaming
    pub fn exec_command_stop(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self.exec_command(Command::CmdStop, false, 0, None) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // exec_command_get_header executes client TCP command to get the header
    pub fn exec_command_get_header(&self) -> Result<HeaderEntry, Box<dyn std::error::Error>> {
        match self.exec_command(Command::CmdHeader, false, 0, None) {
            Ok((header, _)) => Ok(header),
            Err(e) => Err(e),
        }
    }

    // exec_command_get_entry executes client TCP command to get an entry
    pub fn exec_command_get_entry(
        &self,
        from_entry: u64,
    ) -> Result<Entry, Box<dyn std::error::Error>> {
        match self.exec_command(Command::CmdEntry, false, from_entry, None) {
            Ok((_, entry)) => Ok(entry),
            Err(e) => Err(e),
        }
    }

    // exec_command_get_bookmark executes client TCP command to get a bookmark
    pub fn exec_command_get_bookmark(
        &self,
        from_bookmark: Vec<u8>,
    ) -> Result<Entry, Box<dyn std::error::Error>> {
        match self.exec_command(Command::CmdBookmark, false, 0, Some(from_bookmark)) {
            Ok((_, entry)) => Ok(entry),
            Err(e) => Err(e),
        }
    }

    // exec_command executes a valid client TCP command with deferred command result possibility
    fn exec_command(
        &mut self,
        cmd: Command,
        deferred_result: bool,
        from_entry: u64,
        from_bookmark: Option<Vec<u8>>,
    ) -> Result<(HeaderEntry, Entry), ClientError> {
        println!(
            "{} Executing command {:?}...",
            self.id,
            cmd,
        );
        let mut header: HeaderEntry;
        let mut entry: Entry;

        // Check status of the client
        if !self.started {
            println!("Execute command not allowed. Client is not started");
            return Err(Box::new(Error::new(
                ErrorKind::Other,
                "Execute command not allowed. Client is not started",
            )));
        }

        // Check valid command
        if !cmd.is_a_command() {
            println!("{} Invalid command {}", self.id, cmd);
            return Err(Box::new(Error::new(ErrorKind::Other, "Invalid command")));
        }

        // Send command
        self.conn.write_all(&(cmd as u64).to_le_bytes())?;

        // Send stream type
        self.conn
            .write_all(&(self.stream_type as u64).to_le_bytes())?;

        // Send the command parameters
        match cmd {
            Command::CmdStart => {
                println!("{} ...from entry {}", self.id, from_entry);
                // Send starting/from entry number
                self.conn.write_all(&from_entry.to_le_bytes())?;
            }
            Command::CmdStartBookmark => {
                println!("{} ...from bookmark {:?}", self.id, from_bookmark);
                // Send starting/from bookmark length
                if let Some(bookmark) = &from_bookmark {
                    self.conn
                        .write_all(&(bookmark.len() as u32).to_le_bytes())?;
                    // Send starting/from bookmark
                    self.conn.write_all(bookmark)?;
                }
            }
            Command::CmdEntry => {
                println!("{} ...get entry {}", self.id, from_entry);
                // Send entry to retrieve
                self.conn.write_all(&from_entry.to_le_bytes())?;
            }
            Command::CmdBookmark => {
                println!("{} ...get bookmark {:?}", self.id, from_bookmark);
                // Send bookmark length
                if let Some(bookmark) = &from_bookmark {
                    self.conn
                        .write_all(&(bookmark.len() as u32).to_le_bytes())?;
                    // Send bookmark to retrieve
                    self.conn.write_all(bookmark)?;
                }
            }
            _ => {}
        }

        // Get the command result
        if !deferred_result {
            let r = self.get_result(cmd);
            if r.error_num != Command::CmdErrOK as u32 {
                return Err(Box::new(Error::new(
                    ErrorKind::Other,
                    "Result command error",
                )));
            }
        }

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
                let h = self.get_header();
                header = h;
                self.total_entries = header.total_entries;
            }
            Command::CmdEntry => {
                let e = self.get_entry();
                if e.entry_type == EntryType::EntryTypeNotFound {
                    return Err(Box::new(Error::new(ErrorKind::Other, "Entry not found")));
                }
                entry = e;
            }
            Command::CmdBookmark => {
                let e = self.get_entry();
                if e.entry_type == EntryType::EntryTypeNotFound {
                    return Err(Box::new(Error::new(ErrorKind::Other, "Bookmark not found")));
                }
                entry = e;
            }
            _ => {}
        }

        Ok((header, entry))
    }
}

fn print_received_entry(entry: Entry) -> Result<(), Box<dyn std::error::Error>> {
    println!("Received entry: {:?}", entry);
    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_stream_client_new() {
//         let server = "localhost:6000".to_string();
//         let stream_type = StreamType::Sequencer;
//         let client = StreamClient::new(server, stream_type).unwrap();
//         assert_eq!(client.server, server);
//         assert_eq!(client.stream_type, stream_type);
//     }
// }
