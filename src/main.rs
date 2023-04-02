extern crate tokio;
extern crate hyper;
extern crate multer;
extern crate r2d2;
extern crate r2d2_sqlite;
extern crate rusqlite;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate mime_guess;

use std::borrow::BorrowMut;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use std::sync::Mutex;
use hyper::{Body, Error, Method, Request, Response, Server, StatusCode};
use hyper::header::{CONTENT_TYPE, HeaderName};
use hyper::http::HeaderValue;
use hyper::service::{make_service_fn, service_fn};
use multer::Multipart;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use serde_json::Value;
use tokio::time::sleep;
use crate::blob::{BlobCounters, Block};
use crate::config::Config;

mod config;
mod blob;

//to easily make response
fn make_resp(resp: Value, code: StatusCode) -> Response<Body> {
    return Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .status(code)
        .body(resp.to_string().into()).unwrap();
}

// Process the request body as multipart/form-data.
async fn process_multipart(body: Body, boundary: String) -> multer::Result<(Vec<u8>, Option<String>)> {
    // Create a Multipart instance from the request body.
    let mut buf = vec![];
    let mut multipart = Multipart::new(body, boundary);
    let mut file= None;

    while let Some(mut field) = multipart.next_field().await? { //recieve file
        let name = field.name();
        let file_name = field.file_name();
        let content_type = field.content_type();

        match file_name {
            Some(f) => file = Some(f.to_string()),
            None => {}
        };

        //println!("Name: {:?}, FileName: {:?}, Content-Type: {:?}", name, file_name, content_type);
        let mut field_bytes_len = 0;
        while let Some(field_chunk) = field.chunk().await? {
            // Do something with field chunk.
            buf.append(&mut field_chunk.to_vec());
            field_bytes_len += field_chunk.len();
        }
        //println!("Field Bytes Length: {}", field_bytes_len);
    }

    let content_type = match file { //determine content type
        Some(f) => Some(mime_guess::from_path(f).first_or_text_plain().to_string()),
        None => None
    };

    Ok((buf, content_type))
}
//get blob content using block id
fn get_blob_blockid(conn: PooledConnection<SqliteConnectionManager>, _req: Request<Body>, path: Vec<String>, config: &Config) -> Result<Response<Body>, Infallible> {
    if path.len() < 1 {
        return Ok(make_resp(json!({ "err": "missing block_id url parameter" }),StatusCode::BAD_REQUEST ));
    }
    let block_id = match path[0].parse::<i64>() {
        Ok(num) => num,
        Err(e) => {
            return Ok(make_resp(json!({ "err": "error parsing block_id url parameter, must be long int", "exception": format!("{:?}",e) }),StatusCode::BAD_REQUEST ));
        }
    };

    let result = match Block::read(&conn, config, block_id) {
        Ok(res) => res,
        Err(e) => return Ok(make_resp(json!({"err": e}), StatusCode::INTERNAL_SERVER_ERROR))
    };

    Ok (Response::builder()
        .header(CONTENT_TYPE, result.1)
        .status(StatusCode::OK)
        .body(result.0.into())
        .unwrap())
}

//delete blob content using block id
fn delete_blob_blockid(conn: PooledConnection<SqliteConnectionManager>, _req: Request<Body>, path: Vec<String>) -> Result<Response<Body>, Infallible> {
    if path.len() < 1 {
        return Ok(make_resp(json!({ "err": "missing block_id url parameter" }),StatusCode::BAD_REQUEST ));
    }

    let block_id = match path[0].parse::<i64>() {
        Ok(num) => num,
        Err(e) => {
            return Ok(make_resp(json!({ "err": "error parsing block_id url parameter, must be long int", "exception": format!("{:?}",e) }),StatusCode::BAD_REQUEST ));
        }
    };

    match Block::delete(&conn, block_id) {
        Ok(_) => {},
        Err(e) => {
            return Ok(make_resp(json!({"err": e}),StatusCode::INTERNAL_SERVER_ERROR))
        }
    };

    Ok(make_resp(json!({"msg": "deleted"}), StatusCode::OK))
}

//insert new blob
async fn post_blob(conn: PooledConnection<SqliteConnectionManager>, _req: Request<Body>, config: &Config, counters: Arc<Mutex<BlobCounters>>) -> Result<Response<Body>, Infallible> {
    let boundary = _req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| multer::parse_boundary(ct).ok());

    let b = match boundary {
        Some(boundary) => {
            match process_multipart(_req.into_body(), boundary).await {
                Ok(b) => b,
                Err(e) => {
                    return Ok(make_resp(json!({ "err": "error receiving request", "exception": format!("{:?}",e) }),StatusCode::BAD_REQUEST ));
                }
            }
        },
        None => {
            match hyper::body::to_bytes(_req).await {
                Ok(b) => (b.to_vec(), Some("application/octet-stream".to_string())),
                Err(e) => {
                    return Ok(make_resp(json!({ "err": "error receiving request", "exception": format!("{:?}",e) }),StatusCode::BAD_REQUEST ));
                }
            }
        },
    };

    let block_id = match Block::write(&conn, config, counters, b.0, b.1) {
        Ok(id) => id,
        Err(e) => return Ok(make_resp(json!({ "err": "error uploading blob", "exception": e }),StatusCode::BAD_REQUEST ))
    };

    Ok(make_resp(json!({"block_id": block_id}), StatusCode::OK))
}

pub async fn preflight(req: Request<Body>, config: Config) -> Result<Response<Body>, Infallible> {
    let _whole_body = hyper::body::aggregate(req).await.unwrap();
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::default()).unwrap();

    let mut h = response.headers_mut();
    for cors_h in config.CORS {
        h.insert::<HeaderName>(cors_h.key.parse().unwrap(), cors_h.val.parse().unwrap());
    }
    Ok(response)
}

//handle http requests
async fn service(conn: PooledConnection<SqliteConnectionManager>, config: Config, counters: Arc<Mutex<BlobCounters>>, _req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let path_str = _req.uri().path().to_string();
    let path: Vec<String> = path_str.split("/").filter(|str| str.len() > 0).map(|str| str.to_string()).collect();

    println!("{} {}", _req.method(), _req.uri().path());

    let mut response = match _req.method() {
        &Method::OPTIONS => preflight(_req, config.clone()).await?,
        &Method::GET => { get_blob_blockid(conn, _req, path, &config)? },
        &Method::DELETE => { delete_blob_blockid(conn, _req, path)? },
        &Method::POST => { post_blob(conn, _req, &config, counters).await? },
        _=> { make_resp(json!({"err":"not found"}), StatusCode::NOT_FOUND) }
    };

    let mut h = response.headers_mut();
    for cors_h in config.CORS {
        h.insert::<HeaderName>(cors_h.key.parse().unwrap(), cors_h.val.parse().unwrap());
    }

    Ok(response)
}

#[tokio::main]
async fn main() {
    let mut config = Config::new();

    let addr = ([127, 0, 0, 1], config.port).into();

    let manager = SqliteConnectionManager::file(format!("{}/data.db",config.data_dir).as_str());
    let pool = r2d2::Pool::new(manager).unwrap();

    let mut counters: Arc<Mutex<BlobCounters>> = Arc::new(Mutex::new((0, 0, 0)));
    {
        let conn = pool.get().unwrap();
        let mut lock = counters.lock().unwrap();
        *lock = Block::init(&conn, config.data_dir.as_str()).unwrap();
    }

    let clean_pool = pool.clone();
    let clean_config = config.clone();
    let clean_counters = counters.clone();
    tokio::spawn(async move {
        loop {
            println!("clean started");
            let db = clean_pool.get().unwrap();
            let counters = clean_counters.clone();
            Block::clean(&db, &clean_config, counters).unwrap();
            println!("clean completed");
            sleep(Duration::from_secs(clean_config.clean_interval_secs)).await;
        }
    });

    let make_service = make_service_fn(move |_| {
        let pool = pool.clone();
        let counters = counters.clone();
        let config = config.clone();

        async move {
            Ok::<_, Error>(service_fn(move |_req| {
                let conn = pool.get().unwrap();
                service(conn, config.clone(), counters.clone(), _req)
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_service);

    println!("listening on http://{}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}
