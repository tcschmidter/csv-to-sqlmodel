use std::collections::HashMap;
use std::sync::Arc;
use rayon::prelude::*;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use uuid::Uuid;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

mod generate_csv;

fn main() -> std::io::Result<()> {
    let filename = "big_csv.csv";
    // Generate the CSV file
    
    // let num_rows = 1_000_000;
    // generate_csv::generate_csv(filename, num_rows)?;

    // Parse the CSV file
    csv_parser(filename, ",", true)?;

    Ok(())
}

fn csv_parser(
    filename: &str,
    delimiter: &str,
    has_header: bool,
) -> std::io::Result<()> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines().peekable();

    let mut headers: Vec<String> = Vec::new();
    let mut columns: Vec<Vec<String>> = Vec::new();

    if has_header {
        if let Some(Ok(header_line)) = lines.next() {
            headers = header_line
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
            columns.resize(headers.len(), Vec::new());
        }
    }

    const BATCH_SIZE: usize = 1000;
    let mut batch: Vec<String> = Vec::with_capacity(BATCH_SIZE);

    while lines.peek().is_some() {
        batch.clear();
        for _ in 0..BATCH_SIZE {
            if let Some(Ok(line)) = lines.next() {
                batch.push(line);
            } else {
                break;
            }
        }

        // Transpose the batch into columns
        let transposed_batch = transpose_batch(&batch, delimiter, headers.len());

        // Merge the transposed batch into the main columns
        for (i, col) in transposed_batch.into_iter().enumerate() {
            columns[i].extend(col);
        }
    }

    let mut column_types: HashMap<String, String> = HashMap::new();

    for (i, column) in columns.into_iter().enumerate() {
        let column_name = headers.get(i).cloned().unwrap_or_else(|| format!("col{}", i + 1));
        let inferred_type = infer_column_type(column, delimiter);
        column_types.insert(column_name, inferred_type);
    }

    println!("Inferred column types: {:?}", column_types);
    Ok(())
}

fn transpose_batch(batch: &[String], delimiter: &str, num_columns: usize) -> Vec<Vec<String>> {
    let mut transposed: Vec<Vec<String>> = vec![Vec::new(); num_columns];

    for line in batch {
        let fields: Vec<&str> = line.split(delimiter).collect();
        for (i, field) in fields.iter().enumerate() {
            transposed[i].push(field.to_string());
        }
    }

    transposed
}

fn infer_column_type(mut column: Vec<String>, delimiter: &str) -> String {
    const BATCH_SIZE: usize = 1000;
    let num_batches = (column.len() + BATCH_SIZE - 1) / BATCH_SIZE;

    let batches: Vec<_> = (0..num_batches)
        .map(|i| {
            let start = i * BATCH_SIZE;
            let end = (start + BATCH_SIZE).min(column.len());
            column[start..end].to_vec()
        })
        .collect();

    let types: Vec<_> = batches.par_iter().map(|batch| {
        let mut local_types: HashMap<String, usize> = HashMap::new();

        for field in batch {
            let inferred_type = infer_sql_type(field);
            *local_types.entry(inferred_type).or_insert(0) += 1;
        }

        local_types
    }).collect();

    let mut combined_types: HashMap<String, usize> = HashMap::new();
    for t in types {
        for (type_name, count) in t {
            *combined_types.entry(type_name).or_insert(0) += count;
        }
    }

    determine_loosest_type(combined_types)
}

fn determine_loosest_type(types: HashMap<String, usize>) -> String {
    let mut type_order = vec![
        ("BIT", 0),
        ("TINYINT", 1),
        ("SMALLINT", 2),
        ("INT", 3),
        ("BIGINT", 4),
        ("REAL", 5),
        ("FLOAT", 6),
        ("DATE", 7),
        ("TIME", 8),
        ("DATETIME", 9),
        ("DATETIME2", 10),
        ("UNIQUEIDENTIFIER", 11),
        ("CHAR", 12),
        ("VARCHAR(MAX)", 13),
        ("NCHAR", 14),
        ("NVARCHAR(MAX)", 15),
    ];

    type_order.sort_by_key(|&(_, order)| order);

    for &(type_name, _) in &type_order {
        if types.contains_key(type_name) {
            return type_name.to_string();
        }
    }

    "NVARCHAR(MAX)".to_string()
}

fn infer_sql_type(value: &str) -> String {
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return "BIT".to_string();
    }

    if let Ok(_) = value.parse::<i8>() {
        return "TINYINT".to_string();
    }

    if let Ok(_) = value.parse::<i16>() {
        return "SMALLINT".to_string();
    }

    if let Ok(_) = value.parse::<i32>() {
        return "INT".to_string();
    }

    if let Ok(_) = value.parse::<i64>() {
        return "BIGINT".to_string();
    }

    if let Ok(parsed_float) = value.parse::<f32>() {
        if parsed_float.to_string() == value {
            return "REAL".to_string();
        }
    }

    if let Ok(_) = value.parse::<f64>() {
        return "FLOAT".to_string();
    }

    if let Ok(_) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return "DATE".to_string();
    }

    if let Ok(_) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return "DATETIME".to_string();
    }

    if let Ok(_) = NaiveTime::parse_from_str(value, "%H:%M:%S") {
        return "TIME".to_string();
    }

    if let Ok(_) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return "DATETIME2".to_string();
    }

    if let Ok(_) = Uuid::parse_str(value) {
        return "UNIQUEIDENTIFIER".to_string();
    }

    if value.chars().all(|c| c.is_ascii()) && value.len() == value.chars().count() {
        return "CHAR".to_string();
    }

    "NVARCHAR(MAX)".to_string()
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
        assert_eq!(infer_sql_type("-123.45"), "REAL");
        assert_eq!(infer_sql_type("123453345334523455555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555554534534534545343.4567345645645645643333333333333333334234234234234234234234234233564564564564589"), "FLOAT");
        assert_eq!(infer_sql_type("2023-10-05"), "DATE");
        assert_eq!(infer_sql_type("2023-10-05 14:30:00"), "DATETIME");
        assert_eq!(infer_sql_type("14:30:00"), "TIME");
        assert_eq!(infer_sql_type("2023-10-05 14:30:00.123456"), "DATETIME2");
        assert_eq!(infer_sql_type("123e4567-e89b-12d3-a456-426614174000"), "UNIQUEIDENTIFIER");
        assert_eq!(infer_sql_type("some text"), "NVARCHAR(MAX)");
        assert_eq!(infer_sql_type("abc"), "CHAR");
    }

    #[test]
    fn test_csv_parser_with_header() -> io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_str().unwrap();
        let mut file = File::create(file_path)?;
        writeln!(file, "id,name,age,is_active,created_at,created_time,uuid")?;
        writeln!(file, "1,Alice,30,true,2023-10-05 14:30:00,14:30:00,123e4567-e89b-12d3-a456-426614174000")?;
        writeln!(file, "2,Bob,25,false,2023-10-06 15:45:00,15:45:00,123e4567-e89b-12d3-a456-426614174001")?;

        csv_parser(file_path, ",", true)?;

        Ok(())
    }

    #[test]
    fn test_csv_parser_without_header() -> io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_str().unwrap();
        let mut file = File::create(file_path)?;
        writeln!(file, "1,Alice,30,true,2023-10-05 14:30:00,14:30:00,123e4567-e89b-12d3-a456-426614174000")?;
        writeln!(file, "2,Bob,25,false,2023-10-06 15:45:00,15:45:00,123e4567-e89b-12d3-a456-426614174001")?;

        csv_parser(file_path, ",", false)?;

        Ok(())
    }
}