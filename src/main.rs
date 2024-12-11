fn main() -> std::io::Result<()> {
    csv_parser("test.csv", ",")
}

fn csv_parser(filename: &str, delimiter: &str) -> std::io::Result<()> {
    let mut reader = file_reader::BufReader::open(filename)?;
    let mut buffer = String::new();

    while let Some(line) = reader.read_line(&mut buffer) {
        println!("{}", line?.trim());
    }

    Ok(())
}

mod file_reader {
    use std::{
        fs::File,
        io::{self, prelude::*},
    };

    pub struct BufReader {
        reader: io::BufReader<File>,
    }

    impl BufReader {
        pub fn open(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
            let file = File::open(path)?;
            let reader = io::BufReader::new(file);

            Ok(Self { reader })
        }

        pub fn read_line<'buf>(
            &mut self,
            buffer: &'buf mut String,
        ) -> Option<io::Result<&'buf mut String>> {
            buffer.clear();

            self.reader
                .read_line(buffer)
                .map(|u| if u == 0 { None } else { Some(buffer) })
                .transpose()
        }
    }
}