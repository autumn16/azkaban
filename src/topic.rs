use rocket::State;
use rocket::{serde::json::Json, post};
use serde::{Serialize, Deserialize};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::fmt::Debug;
use rusqlite::{Connection, params};

use crate::{BaseResponseMsg, generate_id, Processing};

use crate::sql_command::{
    CREATE_TOPIC_STMT,
    READ_TOPIC_STMT,
    DELETE_TOPIC_STMT
};

#[derive(Debug)]
pub enum TopicTyp {
    CREATE,
    READ,
    DELETE,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateAndDeleteTopicRequest {
    pub name: String
}

pub trait CDTopicTrait {
    fn get_name(&self) -> String;
}

impl CDTopicTrait for CreateAndDeleteTopicRequest {
    fn get_name(&self) -> String {
        return self.name.clone();
    }
}

/// Create Topic 
#[post("/create", data = "<create_req>")]
pub fn topic_create(create_req: Json<CreateAndDeleteTopicRequest>, conn: &State<Arc<Mutex<Connection>>>) -> Json<BaseResponseMsg> {
    let response = TopicWorker { 
        job_id: generate_id(), 
        typ: TopicTyp::CREATE, 
        request: &create_req, 
        response: BaseResponseMsg {
            msg: "Ok".to_string(),
            detail: "Topic created successfully".to_string()
        },
        conn: conn.inner()
    }.process();
    
    Json(response)
}

/// Delete Topic
#[post("/delete", data = "<delete_req>")]
pub fn topic_delete(delete_req: Json<CreateAndDeleteTopicRequest>, conn: &State<Arc<Mutex<Connection>>>) -> Json<BaseResponseMsg> {
    let response = TopicWorker { 
        job_id: generate_id(), 
        typ: TopicTyp::DELETE, 
        request: &delete_req.name, 
        response: BaseResponseMsg {
            msg: "Ok".to_string(),
            detail: "Topic delete successfully".to_string()
        },
        conn: conn.inner()
    }.process();

    Json(response)
}

#[derive(Serialize, Deserialize)]
pub struct Topic {
    pub id: usize,
    pub name: String,
    pub data: String
}

// #[derive(Debug)]
// #[deprecated]
// pub struct TopicWorker<'a, Request, Response>
// where 
//     Response: Clone + Debug, 
//     Request: Debug
// {
//     pub job_id: usize,
//     pub typ: TopicTyp,
//     pub request: Request,
//     pub response: Response,
//     pub conn: &'a Arc<Mutex<Connection>>,
// }

#[derive(Debug)]
pub struct TopicWorker<'a, Request, BaseResponseMsg>
where 
    Request: Debug
{
    pub job_id: usize,
    pub typ: TopicTyp,
    pub request: Request,
    pub response: BaseResponseMsg,
    pub conn: &'a Arc<Mutex<Connection>>,
}


// /// Implement Processing for Topic *Worker*
// impl<Request, Response> Processing<Request, Response> for TopicWorker<'_, Request, Response> 
// where Response: Clone + Debug, Request: Debug + CDTopicTrait
// {
//     fn process(&self) -> Response {
//         match self.typ {
//             TopicTyp::CREATE => {
//                 // Topic Entity
//                 let topic = Topic {
//                     id: self.job_id,
//                     name: self.request.get_name(),
//                     data: String::from(""),
//                 };

//                 // Insert To SQLite
//                 self.conn.lock()
//                     .expect("[Azkaban] Failed to get resources at \\topic\\create")
//                     .execute(CREATE_TOPIC_STMT, (&topic.id, &topic.name, &topic.data))
//                     .expect("[Azkaban] Failed when creating topic");

//                 println!("[Azkaban] Creating topic {:?}", self);
//             },
//             TopicTyp::READ => {
//                 let topic: Topic = self.conn.lock()
//                     .expect("[Azkaba] Failed to get resources at \\topic\\read")
//                     .query_row(READ_TOPIC_STMT, params![self.job_id], move |row| {
//                         Ok(
//                             Topic {
//                                 id: row.get(0)?,
//                                 name: row.get(1)?,
//                                 data: row.get(2)?
//                             }
//                         )
//                     })
//                     .expect("[Azkaban] Failed when reading topic");

//                 return Response
//             },
//             TopicTyp::DELETE => {
//                 let name = self.request.get_name();

//                 // Delete From SQLite
//                 self.conn.lock()
//                     .expect("[Azkaban] Failed to get resources at \\topic\\create")
//                     .execute(DELETE_TOPIC_STMT, params![name])
//                     .expect("[Azkaban] Failed when creating topic");

//                 println!("[Azkaban] Deleting topic {:?}", self);
//             }
//         }
//         self.response.clone()
//     }
// }

/// Implement Processing for Topic *Worker*
impl<Request> Processing<Request, BaseResponseMsg> for TopicWorker<'_, Request, BaseResponseMsg> 
where Request: Debug + CDTopicTrait
{
    fn process(&self) -> BaseResponseMsg {
        match self.typ {
            TopicTyp::CREATE => {
                // Topic Entity
                let topic = Topic {
                    id: self.job_id,
                    name: self.request.get_name(),
                    data: String::from(""),
                };

                // Insert To SQLite
                self.conn.lock()
                    .expect("[Azkaban] Failed to get resources at \\topic\\create")
                    .execute(CREATE_TOPIC_STMT, (&topic.id, &topic.name, &topic.data))
                    .expect("[Azkaban] Failed when creating topic");

                println!("[Azkaban] Creating topic {:?}", self);
            },
            TopicTyp::READ => {
                let topic: Topic = self.conn.lock()
                    .expect("[Azkaba] Failed to get resources at \\topic\\read")
                    .query_row(READ_TOPIC_STMT, params![self.job_id], move |row| {
                        Ok(
                            Topic {
                                id: row.get(0)?,
                                name: row.get(1)?,
                                data: row.get(2)?
                            }
                        )
                    })
                    .expect("[Azkaban] Failed when reading topic");
                
                // This case is ugly, the structure of the project will be fixed later
                return BaseResponseMsg {
                    msg: "Ok".to_string(),
                    detail: format!("{}", json!(topic)),
                };
            },
            TopicTyp::DELETE => {
                let name = self.request.get_name();

                // Delete From SQLite
                self.conn.lock()
                    .expect("[Azkaban] Failed to get resources at \\topic\\create")
                    .execute(DELETE_TOPIC_STMT, params![name])
                    .expect("[Azkaban] Failed when creating topic");

                println!("[Azkaban] Deleting topic {:?}", self);
            }
        }
        self.response.clone()
    }
}


impl CDTopicTrait for &Json<CreateAndDeleteTopicRequest> {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

impl CDTopicTrait for &String {
    fn get_name(&self) -> String {
        self.to_string()
    }
}