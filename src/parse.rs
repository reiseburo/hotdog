/**
 * Enum of syslog parse related errors
 */
#[derive(Debug)]
pub enum SyslogErrors {
    UnknownFormat,
}

/**
 * SyslogMessage is just a wrapper struct to allow us to deserialize RFC 5424 and RFC 3164 syslog
 * messages into some format that can be passed throughout hotdog
 */
#[derive(Debug)]
pub struct SyslogMessage {
    pub msg: String,
}

/**
 * Attempt to parse a given line either as RFC 5424 or RFC 3164
 */
pub fn parse_line(line: String) -> std::result::Result<SyslogMessage, SyslogErrors> {
    match syslog_rfc5424::parse_message(&line) {
        Ok(msg) => return Ok(SyslogMessage { msg: msg.msg }),
        Err(_) => {
            let parsed = syslog_loose::parse_message(&line);

            /*
             * Since syslog_loose doesn't give a Result, the only way to tell if themessage wasn't
             * parsed properly is if some fields are None'd out.
             */
            if parsed.timestamp != None {
                return Ok(SyslogMessage {
                    msg: parsed.msg.to_string(),
                });
            }
            Err(SyslogErrors::UnknownFormat)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_invalid() {
        let buffer = "blah".to_string();
        let parsed = parse_line(buffer);
        if let Ok(msg) = &parsed {
            println!("msg: {}", msg.msg);
        }
        assert!(parsed.is_err());
    }

    #[test]
    fn test_5424() {
        let buffer = r#"<13>1 2020-04-18T15:16:09.956153-07:00 coconut tyler - - [timeQuality tzKnown="1" isSynced="1" syncAccuracy="505061"] hi"#.to_string();
        let parsed = parse_line(buffer);
        assert!(parsed.is_ok());
        if let Ok(msg) = parsed {
            assert_eq!("hi", msg.msg);
        }
    }

    #[test]
    fn test_3164() {
        let buffer = r#"<190>May 13 21:45:18 coconut hotdog: hi"#.to_string();
        let parsed = parse_line(buffer);
        assert!(parsed.is_ok());
        if let Ok(msg) = parsed {
            assert_eq!("hi", msg.msg);
        }
    }
}
