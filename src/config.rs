use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::Path;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub log_file: String,
    pub data_dir: String,
    pub max_blob_size_bytes: u64,
    pub blob_clean_interval_secs: u64,
    pub port: u16,
    #[serde(skip_serializing, skip_deserializing)]
    pub version: f32
}

impl Config {
    pub fn new () -> Config {
        let path = "./config/config.json";

        if !Path::new(path).exists() {
            let config = Config {
                log_file: "./log.txt".to_owned(),
                data_dir: "./data".to_owned(),
                port: 3000,
                max_blob_size_bytes: 64*2_u64.pow(20),
                blob_clean_interval_secs: 5*60*60,
                version: 0.0
            };

            match OpenOptions::new().create(true).write(true).open(path.to_owned()) {
                Err(why) => panic!("couldn't open config file: {}", why),
                Ok(mut file) => {
                    file.write_all(serde_json::to_string_pretty(&config).unwrap().as_bytes()).unwrap();
                }
            };

            config

        }else {
            let file = match File::open(path) {
                Ok(file) => file,
                Err(why) => panic!("couldn't open config file: {}", why)
            };

            let reader = BufReader::new(file);
            let config: Config = serde_json::from_reader(reader).unwrap();

            config
        }
    }
}