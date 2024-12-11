fn main() -> std::io::Result<()> {
    csv_parser("test.csv", ",", true)
}

fn csv_parser(
    filename: &str,
    delimiter: &str,
    has_header: bool,
) -> std::io::Result<()> {
    // reads file line by line and splits at delimiter
    // gets col names if has_header, else uses col1, col2, etc
    // performs type inference on values

    let mut reader = file_reader::BufReader::open(filename)?;
    let mut buffer = String::new();
    let mut headers: Vec<String> = Vec::new();

    if has_header {
        // read the first line as headers if has_header
        if let Some(header_line) = reader.read_line(&mut buffer) {
            let header = header_line?;
            headers = header
                .trim()
                .split(delimiter)
                .enumerate()
                .map(|(i, s)| {
                    // Assign default column names if no header name
                    if s.is_empty() {
                        format!("col{}", i + 1)
                    } else {
                        s.to_string()
                    }
                })
                .collect();
            println!("Column names: {:?}", headers);
        }
    }

    while let Some(line) = reader.read_line(&mut buffer) {
        // go line by line and parse fields
        let line = line?;
        let fields: Vec<&str> = line.trim().split(delimiter).collect();
        for (i, field) in fields.iter().enumerate() {
            // get col name or assign default
            let column_name = headers
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col{}", i + 1));

            // basic type inference for int, string, float
            if let Ok(parsed_int) = field.parse::<i64>() {
                println!("{}: {} is an integer", column_name, parsed_int);
            } else if let Ok(parsed_float) = field.parse::<f64>() {
                println!("{}: {} is a float", column_name, parsed_float);
            } else {
                println!("{}: {} is a string", column_name, field);
            }
        }
    }

    Ok(())
}

mod file_reader {
    use std::{
        fs::File,
        io::{self, prelude::*},
    };

    pub struct BufReader {
        // wrapper around io::BufReader to use reusable buffer
        reader: io::BufReader<File>,
    }

    impl BufReader {
        pub fn open(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
            // opens a file and initializes a buffered reader.
            let file = File::open(path)?;
            let reader = io::BufReader::new(file);

            Ok(Self { reader })
        }

        pub fn read_line<'buf>(
            &mut self,
            buffer: &'buf mut String,
        ) -> Option<io::Result<&'buf mut String>> {
            // clears the buffer and reads a single line into it
            // returns None if EOF is reached or Some with the buffer
            buffer.clear();

            self.reader
                .read_line(buffer)
                .map(|u| if u == 0 { None } else { Some(buffer) })
                .transpose()
        }
    }
}
