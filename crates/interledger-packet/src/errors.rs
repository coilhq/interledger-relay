use std::str::Utf8Error;
use std::string::FromUtf8Error;

use quick_error::quick_error;

use super::AddressError;

quick_error! {
    #[derive(Debug)]
    pub enum ParseError {
        Io(err: std::io::Error) {
            from()
            display("Io {}", err)
            cause(err)
        }
        Utf8(err: Utf8Error) {
            from()
            display("Utf8 {}", err)
            cause(err)
        }
        FromUtf8(err: FromUtf8Error) {
            from()
            display("FromUtf8 {}", err)
            cause(err)
        }
        Chrono(err: chrono::ParseError) {
            from()
            display("Chrono {}", err)
            cause(err)
        }
        WrongType(descr: String) {
            display("WrongType {}", descr)
        }
        AddressError(err: AddressError) {
            from()
            display("AddressError {}", err)
            cause(err)
        }
        // TODO &'static str instead of String?
        InvalidPacket(descr: String) {
            display("InvalidPacket {}", descr)
        }
    }
}
