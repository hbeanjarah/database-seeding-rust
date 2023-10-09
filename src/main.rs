extern crate dotenv;

mod common;
mod items;
use dotenv::dotenv;

use std::env;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() {
    dotenv().ok();
    // Attempt to retrieve environment variables
    let database_name = match env::var("DATABASE_NAME") {
        Ok(val) => val,
        Err(_) => {
            eprintln!("Error: DATABASE_NAME environment variable not set");
            return; // Exit the program or handle the error as needed
        }
    };

    let database_host = match env::var("DATABASE_HOST") {
        Ok(val) => val,
        Err(_) => {
            eprintln!("Error: DATABASE_HOST environment variable not set");
            return; // Exit the program or handle the error as needed
        }
    };

    let database_user = match env::var("DATABASE_USER") {
        Ok(val) => val,
        Err(_) => {
            eprintln!("Error: DATABASE_USER environment variable not set");
            return; // Exit the program or handle the error as needed
        }
    };

    let database_password = match env::var("DATABASE_PASSWORD") {
        Ok(val) => val,
        Err(_) => {
            eprintln!("Error: DATABASE_PASSWORD environment variable not set");
            return; // Exit the program or handle the error as needed
        }
    };

    // Construct the database URL
    let database_url = format!(
        "postgres://{}:{}@{}/{}",
        database_user, database_password, database_host, database_name
    );

    println!("database url {:?}", database_url);
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("Error connecting to the database");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let insert_company: bool;

    insert_company = true;

    if insert_company {
        let linkedin_company_file_path = Path::new("./src/bdd/linkedin_interest.json");
        let mut linkedin_company_file =
            File::open(linkedin_company_file_path).expect("Error opening file");
        let mut linkedin_company_file_path_json_data = String::new();

        linkedin_company_file
            .read_to_string(&mut linkedin_company_file_path_json_data)
            .expect("Error reading file");

        let linkedin_company_data: Vec<common::ParentEntity> =
            serde_json::from_str(&linkedin_company_file_path_json_data)
                .expect("Error parsing JSON");

        // println!("linkedin_company_data {:?}", linkedin_company_data);
        for (index, el) in linkedin_company_data.iter().enumerate() {
            if let Err(e) = items::linkedin_company::insert(&client, Some(index as i32), el).await {
                eprintln!("Error inserting data: {}", e);
            }
        }
    } else {
        println!("is working fine")
    }

    let insert_location: bool;
    insert_location = true;

    if insert_location {
        let location_file_path = Path::new("./src/locations.json");
        let mut location_file = File::open(location_file_path).expect("Error opening file");
        let mut location_file_path_json_data = String::new();

        location_file
            .read_to_string(&mut location_file_path_json_data)
            .expect("Error reading file");

        let linkedin_company_data: Vec<common::Location> =
            serde_json::from_str(&location_file_path_json_data).expect("Error parsing JSON");

        // println!("linkedin_company_data {:?}", linkedin_company_data);
        for el in &linkedin_company_data {
            if let Err(e) = items::location_insert::insert_data(&client, &el, None).await {
                eprintln!("Error inserting data: {}", e);
            }
        }
    } else {
        println!("is working fine")
    }
}
