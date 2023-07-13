
pub mod sql_command;
pub mod topic;
pub mod consumer;
pub mod producer;

use rocket::{launch, routes, get};
use serde::{Serialize, Deserialize};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::fmt::Debug;
use rusqlite::Connection;

use crate::topic::{
    topic_create,
    topic_delete
};

use crate::consumer::{
    topic_subscribe,
    consumer_create,
    consumer_consume
};

static INDEX_ROUTE: &str = "/";
static TOPIC_ROUTE: &str = "/topic";
static PRODUCER_ROUTE: &str = "/producer";
static CONSUMER_ROUTE: &str = "/consumer";

static SQLITE_PATH: &str = "./data/atlas.db";

#[launch]
fn rocket() -> _ {
    println!("[Azkaban] Service Initialized!");

    let conn = Connection::open(SQLITE_PATH)
        .expect("[Azkaban] Connect to SQLite Database Failed!");
    let conn_ptr = Arc::new(Mutex::new(conn));

    println!("[Azkaban] SQLite Database Connected!");

    rocket::build()
        .mount(INDEX_ROUTE, routes![index_routes])    
        .mount(TOPIC_ROUTE, routes![topic_create, topic_subscribe, topic_delete])
        .mount(CONSUMER_ROUTE, routes![consumer_create, consumer_consume])
        .mount(PRODUCER_ROUTE, routes![])
        .manage(conn_ptr)
        
}

fn generate_id() -> usize {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    return since_the_epoch.as_secs() as usize;
}

trait Processing<Request, Response> 
where Response: Clone
{
    fn process(&self) -> Response;
}

#[get("/")]
fn index_routes() -> &'static str {
    "[Azkaban] Service Is Running!\n Build With 🧋 By Minh Tri"
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BaseResponseMsg {
    msg: String,
    detail: String,
}

trait AZSync {
    fn sync(&self);
}
