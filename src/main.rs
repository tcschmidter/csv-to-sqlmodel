use std::collections::HashMap;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use uuid::Uuid;

mod generate_csv;

fn main() -> std::io::Result<()> {
    csv_parser("test2.csv", ",", true)
}

fn csv_parser(
    filename: &str,
    delimiter: &str,
    has_header: bool,
) -> std::io::Result<()> {
    let mut reader = file_reader::BufReader::open(filename)?;
    let mut buffer = String::new();
    let mut headers: Vec<String> = Vec::new();
    let mut column_types: HashMap<String, String> = HashMap::new();

    if has_header {
        if let Some(header_line) = reader.read_line(&mut buffer)? {
            let header = header_line;
            headers = header
                .trim()
                .split(delimiter)
                .enumerate()
                .map(|(i, s)| {
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

    while let Some(line) = reader.read_line(&mut buffer)? {
        let line = line;
        let fields: Vec<&str> = line.trim().split(delimiter).collect();
        for (i, field) in fields.iter().enumerate() {
            let column_name = headers
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col{}", i + 1));

            let inferred_type = infer_sql_type(field);
            column_types.entry(column_name.clone()).or_insert_with(|| inferred_type.clone());
            if column_types.get(&column_name) != Some(&inferred_type) {
                column_types.insert(column_name.clone(), "NVARCHAR(MAX)".to_string());
            }

            println!("{}: {} is inferred as {}", column_name, field, inferred_type);
        }
    }

    println!("Inferred column types: {:?}", column_types);
    Ok(())
}

fn infer_sql_type(value: &str) -> String {
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return "BIT".to_string();
    }

    if let Ok(_parsed_int) = value.parse::<i8>() {
        return "TINYINT".to_string();
    }

    if let Ok(_parsed_int) = value.parse::<i16>() {
        return "SMALLINT".to_string();
    }

    if let Ok(_parsed_int) = value.parse::<i32>() {
        return "INT".to_string();
    }

    if let Ok(_parsed_int) = value.parse::<i64>() {
        return "BIGINT".to_string();
    }

    if let Ok(_parsed_float) = value.parse::<f32>() {
        return "REAL".to_string();
    }

    if let Ok(_parsed_float) = value.parse::<f64>() {
        return "FLOAT".to_string();
    }

    if let Ok(_parsed_date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return "DATE".to_string();
    }

    if let Ok(_parsed_datetime) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return "DATETIME".to_string();
    }

    if let Ok(_parsed_time) = NaiveTime::parse_from_str(value, "%H:%M:%S") {
        return "TIME".to_string();
    }

    if let Ok(_parsed_timestamp) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return "DATETIME2".to_string();
    }

    if let Ok(_parsed_uuid) = Uuid::parse_str(value) {
        return "UNIQUEIDENTIFIER".to_string();
    }

    // Default to NVARCHAR(MAX) if no other type matches
    "NVARCHAR(MAX)".to_string()
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

        pub fn read_line<'buf>(&mut self, buffer: &'buf mut String) -> io::Result<Option<&'buf str>> {
            buffer.clear();
            let bytes_read = self.reader.read_line(buffer)?;
            if bytes_read == 0 {
                Ok(None)
            } else {
                Ok(Some(buffer.trim_end()))
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::fs::File;
    use std::io::{self, Write};

    #[test]
    fn test_infer_sql_type() {
        assert_eq!(infer_sql_type("true"), "BIT");
        assert_eq!(infer_sql_type("false"), "BIT");
        assert_eq!(infer_sql_type("127"), "TINYINT");
        assert_eq!(infer_sql_type("32767"), "SMALLINT");
        assert_eq!(infer_sql_type("2147483647"), "INT");
        assert_eq!(infer_sql_type("9223372036854775807"), "BIGINT");
        assert_eq!(infer_sql_type("123.45"), "REAL");
        assert_eq!(infer_sql_type("123.456789"), "FLOAT");
        assert_eq!(infer_sql_type("2023-10-05"), "DATE");
        assert_eq!(infer_sql_type("2023-10-05 14:30:00"), "DATETIME");
        assert_eq!(infer_sql_type("14:30:00"), "TIME");
        assert_eq!(infer_sql_type("2023-10-05 14:30:00.123456"), "DATETIME2");
        assert_eq!(infer_sql_type("123e4567-e89b-12d3-a456-426614174000"), "UNIQUEIDENTIFIER");
        assert_eq!(infer_sql_type("some text"), "NVARCHAR(MAX)");
    }

    #[test]
    fn test_csv_parser_with_header() -> io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_str().unwrap();
        let mut file = File::create(file_path)?;
        writeln!(file, "id,name,age,is_active,created_at")?;
        writeln!(file, "1,Alice,30,true,2023-10-05 14:30:00")?;
        writeln!(file, "2,Bob,25,false,2023-10-06 15:45:00")?;

        csv_parser(file_path, ",", true)?;

        Ok(())
    }

    #[test]
    fn test_csv_parser_without_header() -> io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_str().unwrap();
        let mut file = File::create(file_path)?;
        writeln!(file, "1,Alice,30,true,2023-10-05 14:30:00")?;
        writeln!(file, "2,Bob,25,false,2023-10-06 15:45:00")?;

        csv_parser(file_path, ",", false)?;

        Ok(())
    }
}