use std::{collections::HashMap, env, error::Error};

use clap::{command, Parser, Subcommand};
use dotenvy::dotenv;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use sha2::{Digest, Sha256};
use sqlx::{query, query_scalar, Pool, Postgres};

fn hash_str(s: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(s);
    let result = hasher.finalize();
    // Convert the first 8 bytes of the 32-byte hash result into a fixed-size array of 8 bytes.
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&result[..8]); // Copy the first 8 bytes of the hash result.
    bytes
}

// Generate a short ID using the SHA-256 hash of the URL (first 5 chars)
fn generate_short_id() -> String {
    let id: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    id.to_uppercase() // Convert to uppercase for consistency
}

// Get shard number using consistent hashing
fn get_shard_id(url_id: &str, num_shards: usize) -> usize {
    let hash_value = u64::from_be_bytes(hash_str(url_id));
    (hash_value % num_shards as u64) as usize
}

// Insert the URL into the appropriate shard
async fn insert_url(
    pool_map: &HashMap<usize, Pool<Postgres>>,
    url: &str,
    url_id: &str,
) -> Result<(), sqlx::Error> {
    let shard_id = get_shard_id(url_id, pool_map.len());
    let pool = pool_map.get(&shard_id).unwrap();

    query!(
        "INSERT INTO url_table (url, url_id) VALUES ($1, $2)",
        url,
        url_id
    )
    .execute(pool)
    .await?;

    println!("Inserted URL '{}' into shard {}", url, shard_id + 1);
    Ok(())
}

async fn get_url(
    pool_map: &HashMap<usize, Pool<Postgres>>,
    url_id: &str,
) -> Result<String, Box<dyn Error>> {
    let shard_id = get_shard_id(url_id, pool_map.len());
    let pool = pool_map.get(&shard_id).unwrap();

    Ok(
        query_scalar!("SELECT url FROM url_table WHERE url_id = $1", url_id)
            .fetch_optional(pool)
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
    // Create connection pools for each shard (3 shards in this case)
    let mut pool_map = HashMap::new();

    let pool1 = Pool::<Postgres>::connect(&env::var("DATABASE_URL1").unwrap()).await?;
    let pool2 = Pool::<Postgres>::connect(&env::var("DATABASE_URL2").unwrap()).await?;
    let pool3 = Pool::<Postgres>::connect(&env::var("DATABASE_URL3").unwrap()).await?;

    pool_map.insert(0, pool1);
    pool_map.insert(1, pool2);
    pool_map.insert(2, pool3);

    // Parse command line arguments using `clap`
    let cli = Cli::parse();

    // Match on the command (Insert or Get)
    match &cli.command {
        Commands::Insert { url } => {
            let url_id = generate_short_id();
            insert_url(&pool_map, &url, &url_id).await?;
            println!("URL ID: {}", url_id);
        }
        Commands::Get { url_id } => {
            if let Ok(retrieved_url) = get_url(&pool_map, &url_id).await {
                println!("Retrieved URL: {}", retrieved_url);
            } else {
                println!("URL not found");
            }
        }
    }

    Ok(())
}
