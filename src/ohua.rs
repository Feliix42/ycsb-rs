use crate::db::DB;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};

#[derive(Serialize, Clone, Debug)]
pub enum Message<'a> {
    Read(RequestMsg<'a>),
    Write(Record<'a>),
}

#[derive(Serialize, Clone, Debug)]
pub struct RequestMsg<'a> {
    pub table: &'a str,
    pub key: &'a str,
}

#[derive(Serialize, Clone, Debug)]
pub struct Record<'a> {
    pub table: &'a str,
    pub key: &'a str,
    pub value: &'a HashMap<&'a str, String>,
}

impl<'a> Into<Message<'a>> for Record<'a> {
    fn into(self) -> Message<'a> {
        Message::Write(self)
    }
}

impl<'a> Into<Message<'a>> for RequestMsg<'a> {
    fn into(self) -> Message<'a> {
        Message::Read(self)
    }
}

#[derive(Deserialize, Clone, Debug)]
struct ResponseMsg {
    #[allow(dead_code)]
    pub table: String,
    #[allow(dead_code)]
    pub key: String,
    pub value: HashMap<String, String>
}

pub struct Ohua {
    address: SocketAddr,
}

impl Ohua {
    pub fn new() -> Self {
        Self {
            address: "127.0.0.1:8080".parse().unwrap(),
        }
    }
}

impl DB for Ohua {
    fn init(&self) -> anyhow::Result<()> {
        // Nothing to do?
        Ok(())
    }

    fn insert(&self, table: &str, key: &str, values: &HashMap<&str, String>) -> anyhow::Result<()> {
        let req: Message = Record {
            table,
            key,
            value: values,
        }
        .into();
        
        let s = TcpStream::connect(self.address)?;
        
        serde_json::to_writer(s, &req)?;
        
        Ok(())
    }

    fn read(
        &self,
        table: &str,
        key: &str,
        result: &mut HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let req: Message = RequestMsg { table, key }.into();
        
        let mut s = TcpStream::connect(self.address)?;
        serde_json::to_writer(&mut s, &req)?;
        
        let resp: ResponseMsg = serde_json::from_reader(s)?;
        *result = resp.value;
        
        Ok(())
    }
}
