use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rand::Rng;
use uuid::Uuid;
use std::fs::File;
use std::io::{self, Write};

pub fn generate_million_row_csv(file_path: &str) -> io::Result<()> {
    let mut file = File::create(file_path)?;

    writeln!(file, "id,name,age,is_active,created_at")?;

    for i in 1..=1_000_000 {
        let name = generate_random_name();
        let age = rand::thread_rng().gen_range(18..=99);
        let is_active = rand::thread_rng().gen_bool(0.5);
        let created_at = generate_random_datetime();

        writeln!(file, "{},{},{},{},{}", i, name, age, is_active, created_at)?;
    }

    println!("CSV file with 1 million rows generated successfully.");
    Ok(())
}

fn generate_random_name() -> String {
    let first_names = vec!["Alice", "Bob", "Charlie", "David", "Eve"];
    let last_names = vec!["Smith", "Johnson", "Williams", "Brown", "Jones"];

    let first_name = first_names[rand::thread_rng().gen_range(0..first_names.len())];
    let last_name = last_names[rand::thread_rng().gen_range(0..last_names.len())];

    format!("{} {}", first_name, last_name)
}

fn generate_random_datetime() -> String {
    let start = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let end = Utc::now().naive_utc();
    let duration = end.signed_duration_since(start);
    let random_days = rand::thread_rng().gen_range(0..duration.num_days());
    let random_seconds = rand::thread_rng().gen_range(0..86400);

    start.checked_add_days(chrono::Days::new(random_days as u64))
         .unwrap()
         .checked_add_signed(chrono::Duration::seconds(random_seconds))
         .unwrap()
         .to_string()
}