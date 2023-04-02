use rusqlite::{NO_PARAMS};
use r2d2_sqlite::SqliteConnectionManager;
use r2d2::PooledConnection;

use std::fs;
use std::io::{Read, Write, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use crate::Config;
use crate::core::datatype::shout_on_err;
use crate::database::Database;

fn count_blobs(dir: &str) -> usize {
    let paths = fs::read_dir(dir).unwrap();
    let blob_str = "blob";
    let mut blob_count = 0;

    for path in paths {
        let name = format!("{}", path.unwrap().path().display());

        let mut k = 0;
        for c in name.chars() {
            if k >= blob_str.len() {
                if !c.is_numeric() {
                    k = 0;
                }
            }else if c == blob_str.as_bytes()[k] as char {
                k += 1;
            }else {
                k = 0;
            }
        }

        if k >= blob_str.len() {
            blob_count += 1;
        }
    }

    return blob_count;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockRef {
    id: u32,
    block_id: u32,
    object_id: u32
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Blob {
    id: u32,
    blob: u32,
    start: u64,
    end: u64,
    mime: String
}

pub type BlobCounters = (usize, i64, u64);

impl Blob {

    pub fn init<T: Database>(db: &T, dir: &str) -> BlobCounters {
        let mut num_blocks = 0usize;
        let mut curr_blob = -1;

        num_blocks = count_blobs(dir);
        curr_blob = (num_blocks as i64) - 1;

        shout_on_err::<()>( db.create_table("blocks".to_string(), &json!({
            "id": "INTEGER PRIMARY KEY",
            "blob": "UNSIGNED INTEGER",
            "start": "UNSIGNED LONG INTEGER",
            "end": "UNSIGNED LONG INTEGER",
            "mime": "TEXT"
        })) ).unwrap();

        shout_on_err::<()>( db.create_table("block_refs".to_string(), &json!({
            "id": "INTEGER PRIMARY KEY",
            "block_id": "INTEGER",
            "object_id": "INTEGER"
        })) ).unwrap();

        return (num_blocks, curr_blob, 0)
    }

    pub fn reference_block<T: Database>(db: &T, block_id: u64, object_id: u64) -> Result<bool, String> {
        shout_on_err::<bool>( db.update("block_refs".to_string(), &json!({"block_id": block_id, "object_id": object_id}), &json!({})) )
    }

    pub fn dereference_block<T: Database>(db: &T, block_id: u64, object_id: u64) -> Result<(), String> {
        shout_on_err::<()>( db.delete("block_refs".to_string(), &json!({"block_id": block_id, "object_id": object_id})) )
    }

    fn is_block_referenced<T: Database>(db: &T, block_id: u64) -> Result<bool, String> {
        let results = shout_on_err::<Vec<BlockRef>>( db.get::<BlockRef>("block_refs".to_string(), &json!({"block_id": block_id}), 0, 1) )?;

        Ok(results.len() > 0)
    }

    pub fn clean<T: Database>(db: &T, config: &Config, counters: Arc<Mutex<BlobCounters>>) -> Result<(), String> {
        let mut block_count = 0;
        {
            let lock = counters.lock().unwrap();
            block_count = (*lock).0;
        }
        for curr_blob in 0..block_count {
            let block_limit = 100;
            let mut block_offset = 0;

            loop {
                let blocks = shout_on_err::<Vec<Blob>>(db.get::<Blob>("blocks".to_string(), &json!({}), block_offset, block_limit))?;
                block_offset += block_limit;
                if blocks.len() == 0 {
                    break;
                }

                let old_file = format!("{}/blob{}", config.data_dir, curr_blob);
                let new_file = format!("{}/blob{}_tmp", config.data_dir, curr_blob);

                let mut old_blob = fs::OpenOptions::new()
                    .read(true)
                    .open(old_file.as_str())
                    .unwrap();

                let mut new_blob = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true) // This is needed to append to file
                    .open(new_file.as_str())
                    .unwrap();

                let mut has_things_to_clean = false;
                for block in blocks {
                    if Blob::is_block_referenced::<T>(&db, block.id as u64)? {
                        has_things_to_clean = true;

                        let new_start = new_blob.metadata().unwrap().len();

                        let mut buf = vec![0u8; (block.end - block.start) as usize];
                        old_blob.seek(SeekFrom::Start(block.start.into())).unwrap();
                        old_blob.read_exact(&mut buf).unwrap();
                        new_blob.write(&buf).unwrap();

                        let new_end = new_blob.metadata().unwrap().len();

                        shout_on_err::<bool>( db.update("blocks".to_string(), &json!({"id": block.id}), &json!({"start": new_start, "end": new_end})) )?;

                    } else {

                        shout_on_err::<()>( db.delete("blocks".to_string(), &json!({"id": block.id})) )?;
                    }
                }

                if has_things_to_clean {
                    fs::remove_file(old_file.as_str()).unwrap();
                    fs::rename(new_file, old_file).unwrap();
                    println!("cleaned blob{}", curr_blob);
                }

            }

        }

        Ok(())
    }

    pub fn write<T: Database>(db: &T, config: &Config, counters: Arc<Mutex<BlobCounters>>, data: Vec<u8>, mime: Option<String>) -> Result<i64, String> {
        let mut curr_blob = 0;
        {
            let mut lock = counters.lock().unwrap();
            if (*lock).2 > config.max_blob_size_bytes || (*lock).1 == -1 {
                (*lock).1 = (*lock).0 as i64;
                (*lock).0 += 1;
            }
            curr_blob = (*lock).1
        }

        let mut blob = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true) // This is needed to append to file
            .open(format!("{}/blob{}", config.data_dir, curr_blob).as_str())
            .unwrap();

        let mimeText = match mime {
            Some(m) => m,
            None => "application/octet-stream".to_string()
        };

        let mut curr_blob_len = blob.metadata().unwrap().len();
        let blobqry = json!({"blob":curr_blob, "start": curr_blob_len, "end": 0, "mime": mimeText});
        shout_on_err::<bool>( db.update("blocks".to_string(), &blobqry, &blobqry ) )?;

        let new_blob = shout_on_err::<Vec<Blob>>( db.get::<Blob>("blocks".to_string(), &blobqry, 0,1) )?;

        let id = new_blob.get(0).unwrap().id;
        blob.write(&data).unwrap();
        curr_blob_len = blob.metadata().unwrap().len();

        shout_on_err::<bool>( db.update("blocks".to_string(), &json!({"id": id}), &json!({"end": curr_blob_len})) )?;
        {
            let mut lock = counters.lock().unwrap();
            (*lock).2 += curr_blob_len;
        }

        Ok(id.into())
    }

    pub fn read<T: Database>(db: &T, config: &Config, id: i64) -> Result<(Vec<u8>, String), String> {
        let results = shout_on_err::<Vec<Blob>>( db.get("blocks".to_string(), &json!({"id": id}), 0, 1) )?;

        for block in results {
            if block.end == 0 {
                return Err("cannot read: faulty block".to_string());
            }

            let mut buf = vec![0u8; (block.end - block.start) as usize];

            let mut blob = fs::OpenOptions::new()
                .read(true)
                .open(format!("{}/blob{}", config.data_dir, block.blob).as_str())
                .unwrap();
            blob.seek(SeekFrom::Start(block.start as u64)).unwrap();
            blob.read_exact(&mut buf).unwrap();

            return Ok( (buf, block.mime) );
        }

        Ok( (vec![], String::new()) )
    }

    pub fn delete<T: Database>(db: &T, id: i64) -> Result<(), String> {
        shout_on_err::<()>( db.delete("block_refs".to_string(), &json!({"block_id": id})) )
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use r2d2::PooledConnection;
    use r2d2_sqlite::SqliteConnectionManager;
    use rusqlite::blob::ZeroBlob;
    use rusqlite::{DatabaseName, NO_PARAMS};
    use crate::{Blob, Config};


    #[test]
    fn read_block() {
        let manager = SqliteConnectionManager::file("./data/data.db");
        let pool = r2d2::Pool::new(manager).unwrap();
        let db = pool.get().unwrap();
        let mut config = Config::new();
        {
            Blob::init::<PooledConnection<SqliteConnectionManager>>(&pool.get().unwrap(), "./data");
        }

        let b = Blob::read::<PooledConnection<SqliteConnectionManager>>(&db, &config, 1).unwrap();
        println!("read {} bytes",b.0.len());
    }
}