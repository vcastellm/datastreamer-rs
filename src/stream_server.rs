use byteorder::{BigEndian, ByteOrder};
use log;
use log::{debug, error};
use std::convert::TryInto;
use std::io;
use std::net;
use std::sync;
use std::time;

// Command type for the TCP client commands
pub type Command = u64;

// ClientStatus type for the status of the client
pub type ClientStatus = u64;

// AOStatus type for the atomic operation internal states
pub type AOStatus = u64;

// EntryType type for the entry event types
pub type EntryType = u32;

// StreamType type for the stream types
pub type StreamType = u64;

// CommandError type for the command responses
pub type CommandError = u32;

// EntryTypeNotFound is the entry type value for CmdEntry/CmdBookmark when entry/bookmark not found
pub const ENTRY_TYPE_NOT_FOUND: u32 = u32::MAX;

pub const MAX_CONNECTIONS: usize = 100; // Maximum number of connected clients
pub const STREAM_BUFFER: usize = 256; // Buffers for the stream channel
pub const MAX_BOOKMARK_LENGTH: usize = 16; // Maximum number of bytes for a bookmark

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

#[derive(Debug, Clone, Copy)]
pub enum ClientStatus {
    CsSyncing = 1,
    CsSynced,
    CsStopped,
    CsKilled = 0xff,
}

#[derive(Debug, Clone, Copy)]
pub enum AOStatus {
    // Atomic operation status
    AoNone = 1,
    AoStarted,
    AoCommitting,
    AoRollbacking = 0xff,
}

// StrClientStatus for client status description
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientStatus {
    CsSyncing,
    CsSynced,
    CsStopped,
    CsKilled,
}

// let mut str_client_status: HashMap<ClientStatus, &str> = HashMap::new();
// str_client_status.insert(ClientStatus::CsSyncing, "Syncing");
// str_client_status.insert(ClientStatus::CsSynced, "Synced");
// str_client_status.insert(ClientStatus::CsStopped, "Stopped");
// str_client_status.insert(ClientStatus::CsKilled, "Killed");

const FIXED_SIZE_RESULT_ENTRY: usize = 9; // Replace with actual value

struct ResultEntry {
    packet_type: u8,
    length: u32,
    error_num: u32,
    error_str: Vec<u8>,
}

enum Command {
    CmdStart, // Replace with actual value
    CmdBookmark, // Replace with actual value
              // Add other commands here
}

impl ResultEntry {
    fn from_bytes(b: &[u8]) -> Result<Self, &'static str> {
        if b.len() < FIXED_SIZE_RESULT_ENTRY {
            error!("Invalid binary result entry");
            return Err("Invalid binary result entry");
        }

        let packet_type = b[0];
        let length = BigEndian::read_u32(&b[1..5]);
        let error_num = BigEndian::read_u32(&b[5..9]);
        let error_str = b[9..].to_vec();

        if error_str.len() as u32 != length - FIXED_SIZE_RESULT_ENTRY as u32 {
            error!("Error decoding binary result entry");
            return Err("Error decoding binary result entry");
        }

        Ok(Self {
            packet_type,
            length,
            error_num,
            error_str,
        })
    }

    fn print(&self) {
        debug!("--- RESULT ENTRY -------------------------");
        debug!("packetType: [{}]", self.packet_type);
        debug!("length: [{}]", self.length);
        debug!("errorNum: [{}]", self.error_num);
        debug!("errorStr: [{}]", String::from_utf8_lossy(&self.error_str));
    }
}

impl Command {
    fn is_a_command(&self) -> bool {
        match self {
            Self::CmdStart..=Self::CmdBookmark => true,
            _ => false,
        }
    }
}
