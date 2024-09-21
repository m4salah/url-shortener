use std::{env, error::Error};

use clap::{command, Parser, Subcommand};
use dotenvy::dotenv;
use hashring::HashRing;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use sqlx::{query, query_scalar, Pool, Postgres};

mod hashring;

// Generate a short ID using the SHA-256 hash of the URL (first 5 chars)
fn generate_short_id() -> String {
    let id: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    id.to_uppercase() // Convert to uppercase for consistency
}

// Insert the URL into the appropriate shard
async fn insert_url(hr: &HashRing<Pool<Postgres>>, url: &str) -> Result<String, sqlx::Error> {
    let url_id = generate_short_id();
    let pool = hr.get_shard(&url_id);

    query!(
        "INSERT INTO url_table (url, url_id) VALUES ($1, $2)",
        url,
        url_id
    )
    .execute(&pool)
    .await?;

    println!("Inserted URL '{}'", url);
    Ok(url_id)
}

async fn get_url(hr: &HashRing<Pool<Postgres>>, url_id: &str) -> Result<String, Box<dyn Error>> {
    let pool = hr.get_shard(&url_id);
    Ok(
        query_scalar!("SELECT url FROM url_table WHERE url_id = $1", url_id)
            .fetch_optional(&pool)
            .await?
            .ok_or("No URL found")?,
    )
}

// Clap command line arguments definition
#[derive(Parser)]
#[command(name = "url_shortener")]
#[command(about = "A URL shortener using sharding with PostgreSQL", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Insert a new URL into the sharded database
    Insert {
        /// The URL to be shortened
        url: String,
    },
    /// Retrieve a URL from the sharded database by its short ID
    Get {
        /// The short ID of the URL
        url_id: String,
    },
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv()?;

    let mut hr = HashRing::new(3);
    let pool1 = Pool::<Postgres>::connect(&env::var("DATABASE_URL1").unwrap()).await?;
    let pool2 = Pool::<Postgres>::connect(&env::var("DATABASE_URL2").unwrap()).await?;
    let pool3 = Pool::<Postgres>::connect(&env::var("DATABASE_URL3").unwrap()).await?;
    hr.add(0, pool1);
    hr.add(1, pool2);
    hr.add(2, pool3);

    // Parse command line arguments using `clap`
    let cli = Cli::parse();

    // Match on the command (Insert or Get)
    match &cli.command {
        Commands::Insert { url } => {
            let url_id = insert_url(&hr, &url).await?;
            println!("URL ID: {}", url_id);
        }
        Commands::Get { url_id } => {
            if let Ok(retrieved_url) = get_url(&hr, &url_id).await {
                println!("Retrieved URL: {}", retrieved_url);
            } else {
                println!("URL not found");
            }
        }
    }

    Ok(())
}
