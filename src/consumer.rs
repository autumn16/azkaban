use rocket::State;
use rocket::{serde::json::Json, post};
use serde::{Serialize, Deserialize};
use std::sync::{Arc, Mutex};
use std::fmt::Debug;
use rusqlite::{Connection, params};

use crate::topic::{TopicWorker, CreateAndDeleteTopicRequest, Topic};
use crate::{Processing, BaseResponseMsg, AZSync, generate_id};
use crate::sql_command::{
    CREATE_CONSUMER_STMT,
    SUBSCRIBE_CONSUMER_STMT,
    GET_OFFSET_CONSUMER_STMT, UPDATE_OFFSET_CONSUMER_STMT
};
use crate::topic::TopicTyp;

#[derive(Debug)]
pub enum ConsumerTyp {
    CREATE,
    DELETE,
    UPDATE,
    SUBSCRIBE,
    UNSUBSCRIBE,
    CONSUME,
}

trait RConsumerTrait {
    fn get_topic(&self) -> usize;
}

impl RConsumerTrait for SubscribeAndConsumeRequest {
    fn get_topic(&self) -> usize {
        self.topic_id
    }
}

impl RConsumerTrait for &Json<SubscribeAndConsumeRequest> {
    fn get_topic(&self) -> usize {
        self.topic_id
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubscribeAndConsumeRequest {
    consumer_id: usize,
    topic_id: usize,
}

/// Subscribe Topic
#[post("/subscribe", data = "<subscribe_req>")]
pub fn topic_subscribe(subscribe_req: Json<SubscribeAndConsumeRequest>, conn: &State<Arc<Mutex<Connection>>>) -> Json<BaseResponseMsg> {
    let response = ConsumerWorker { 
        consumer_id: subscribe_req.consumer_id, 
        typ: ConsumerTyp::SUBSCRIBE, 
        request: &subscribe_req, 
        response: BaseResponseMsg {
            msg: "Ok".to_string(),
            detail: "Topic subscribed successfully".to_string()
        },
        conn: conn.inner()
    }.process();

    Json(response)
}

#[post("/consume", data = "<subscribe_req>")]
pub fn consumer_consume(subscribe_req: Json<SubscribeAndConsumeRequest>, conn: &State<Arc<Mutex<Connection>>>) -> Json<BaseResponseMsg> {
    let response = ConsumerWorker { 
        consumer_id: subscribe_req.consumer_id, 
        typ: ConsumerTyp::CONSUME, 
        request: &subscribe_req, 
        response: BaseResponseMsg {
            msg: "Ok".to_string(),
            detail: "Topic consumed successfully".to_string()
        },
        conn: conn.inner()
    }.process();

    Json(response)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateConsumerRequest {
    name: String
}

impl RConsumerTrait for CreateConsumerRequest {
    fn get_topic(&self) -> usize {
        0
    }
}

impl RConsumerTrait for &Json<CreateConsumerRequest> {
    fn get_topic(&self) -> usize {
        0
    }
}

/// Create Consumer
#[post("/create", data = "<create_req>")]
pub fn consumer_create(create_req: Json<CreateConsumerRequest>, conn: &State<Arc<Mutex<Connection>>>) -> Json<BaseResponseMsg> {
    let response = ConsumerWorker {
        consumer_id: generate_id(),
        typ: ConsumerTyp::CREATE,
        request: &create_req,
        response: BaseResponseMsg {
            msg: "Ok".to_string(),
            detail: "Consumer created successfully".to_string()
        },
        conn: conn.inner()
    }.process();

    Json(response)
}

// #[derive(Debug)]
// pub struct ConsumerWorker<'a, Request, Response>
// where
//     Response: Clone + Debug,
//     Request: Debug
// {
//     consumer_id: usize,
//     typ: ConsumerTyp,
//     request: Request,
//     response: Response,
//     conn: &'a Arc<Mutex<Connection>>,
// }

#[derive(Debug)]
pub struct ConsumerWorker<'a, Request, BaseResponseMsg>
where
    Request: Debug
{
    consumer_id: usize,
    typ: ConsumerTyp,
    request: Request,
    response: BaseResponseMsg,
    conn: &'a Arc<Mutex<Connection>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ConsumerToTopic {
    pub consumer_id: usize,
    pub topic_id: usize,
    pub offset: usize
}

/// Implement Processing for Consumer *Worker*
impl <Request> Processing<Request, BaseResponseMsg> for ConsumerWorker<'_, Request, BaseResponseMsg>
where
    Request: Debug + RConsumerTrait
{
    fn process(&self) -> BaseResponseMsg {
        match self.typ {
            ConsumerTyp::SUBSCRIBE => {
                let topic_id = self.request.get_topic();
                
                // Insert To SQLite, Subscribe Consumer to Topic
                self.conn.lock()
                    .expect("[Azkaban] Failed to get resources at \\consumer\\subscribe")
                    .execute(SUBSCRIBE_CONSUMER_STMT, (&self.consumer_id, &topic_id, 0))
                    .expect("[Azkaban] Failed when subscribing consumer to topic");

                println!("[Azkaban] Subscribing topic {:?}", self);
            },
            ConsumerTyp::UNSUBSCRIBE => {
                println!("[Azkaban] Unsubscribing topic {:?}", self);
            },
            ConsumerTyp::CONSUME => {
                let topic_id = self.request.get_topic();

                let offset = self.conn.lock()
                    .expect("[Azkaban] Failed!")
                    .query_row(GET_OFFSET_CONSUMER_STMT, params![&self.consumer_id, topic_id], move |row| {
                        Ok(
                            ConsumerToTopic {
                                consumer_id: row.get(0)?,
                                topic_id: row.get(1)?,
                                offset: row.get(2)?
                            }
                        )
                    })
                    .expect("[Azkaban] Failed!")
                    .offset;

                // This Request Is Having Non-Ixmpact, Should Be Removed Later
                let request = CreateAndDeleteTopicRequest {
                    name: "".to_string(),
                };
                
                // Begin Consume From Offset To End Of Data
                let consume_data = TopicWorker {
                    job_id: topic_id,
                    typ: TopicTyp::READ,
                    request: request,
                    response: BaseResponseMsg {
                        msg: "Ok".to_string(),
                        detail: "Get data from topic OK!".to_string(),
                    },
                    conn: self.conn,
                }.process();
                
                // Parse
                let topic_data: Topic = serde_json::from_str::<Topic>(&consume_data.detail)
                    .expect("[Azkaban] Parse result failed at \\consumer\\consume");
                
                // Read Data From Offset
                let read_from_offset = &topic_data.data[offset..];

                // Update Offset
                self.conn.lock()
                    .expect("[Azkaban] Failed when getting resources \\consumer\\consume")
                    .execute(UPDATE_OFFSET_CONSUMER_STMT, params![offset + read_from_offset.len(), &self.consumer_id, topic_id])
                    .expect("[Azkaban] Failed when updating offset");

                return BaseResponseMsg {
                    msg: "Ok".to_string(),
                    detail: format!("{}", read_from_offset)
                }

            },
            ConsumerTyp::CREATE => {
                self.conn.lock()
                    .expect("[Azkaban] Failed to get resources at \\consumer\\create")
                    .execute(CREATE_CONSUMER_STMT, params![&self.consumer_id])
                    .expect("[Azkaban] Failed when creating consumer");
            },
            _ => {
                println!("[Azkaban] Invalid consumer type {:?}", self);
            }
        }
        return self.response.clone();
    }
}

struct Consumer {
    id: usize,
}

impl AZSync for Consumer {
    fn sync(&self) {
        println!("Consumer {} is syncing", self.id);
    }
}
