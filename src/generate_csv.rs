use chrono::{NaiveDate, NaiveTime};
use rand::Rng;
use rand::distributions::Alphanumeric;
use std::fs::File;
use std::io::{self, Write};
use uuid::Uuid;

pub fn generate_csv(filename: &str, num_rows: usize) -> io::Result<()> {
    let mut file = File::create(filename)?;

    // Write the header
    writeln!(file, "id,name,description,age,is_active,created_at,created_time,uuid")?;

    let mut rng = rand::thread_rng();

    for i in 0..num_rows {
        let id = i + 1;
        let name = generate_random_string(&mut rng, 10); // Random string of length 10
        let len = rng.gen_range(50..=100);
        let description = generate_random_string(&mut rng, len);
        let age = rng.gen_range(18..=65);
        let is_active = rng.gen_bool(0.5);
        let created_at = generate_random_datetime(&mut rng)?;
        let created_time = generate_random_time(&mut rng)?;
        let uuid = Uuid::new_v4();

        writeln!(
            file,
            "{},{},{},{},{},{},{},{}",
            id, name, description, age, is_active, created_at, created_time, uuid
        )?;
    }

    Ok(())
}

fn generate_random_string(rng: &mut impl Rng, length: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn generate_random_datetime(rng: &mut impl Rng) -> io::Result<String> {
    let start_date = NaiveDate::from_ymd_opt(1980, 1, 1)
        .ok_or(io::Error::new(io::ErrorKind::Other, "Invalid start date"))?;
    let end_date = NaiveDate::from_ymd_opt(2023, 10, 1)
        .ok_or(io::Error::new(io::ErrorKind::Other, "Invalid end date"))?;
    let days_between = (end_date - start_date).num_days();

    let random_days = rng.gen_range(0..=days_between);
    let random_date = start_date + chrono::Duration::days(random_days);

    let hours = rng.gen_range(0..24);
    let minutes = rng.gen_range(0..60);
    let seconds = rng.gen_range(0..60);

    let random_datetime = random_date.and_hms_opt(hours, minutes, seconds)
        .ok_or(io::Error::new(io::ErrorKind::Other, "Invalid datetime"))?;
    Ok(random_datetime.format("%Y-%m-%d %H:%M:%S").to_string())
}

fn generate_random_time(rng: &mut impl Rng) -> io::Result<String> {
    let hours = rng.gen_range(0..24);
    let minutes = rng.gen_range(0..60);
    let seconds = rng.gen_range(0..60);

    let random_time = NaiveTime::from_hms_opt(hours, minutes, seconds)
        .ok_or(io::Error::new(io::ErrorKind::Other, "Invalid time"))?;
    Ok(random_time.format("%H:%M:%S").to_string())
}