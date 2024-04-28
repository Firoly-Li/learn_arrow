use std::io::{self, Write};
use arrow_flight::FlightClient;
use clap::Parser;
use tonic::transport::Channel;
use crate::client::cli::handshake::Handshake;
use anyhow::Result;
use self::cli::{get_schema::GetSchema, Executor, SubCmd};

pub mod cli;

/**
 * arrow_cli
 */
#[derive(Debug, Parser)]
#[command(name = "arrow_cli",version,author,about,long_about = None)]
pub struct Opts {
    #[arg(long)]
    pub host: String,
    #[arg(long)]
    pub port: usize,
}

/**
 * 运行命令行工具
 */
pub async fn run_cli() -> Result<()> {
    let opts = Opts::parse();
    let host = opts.host;
    let port = opts.port;
    let url = format!("http://{}:{}", host, port);
    let my_static_str: &'static str = Box::leak(url.into_boxed_str());
    let channel = Channel::from_static(my_static_str).connect().await?;
    let client = FlightClient::new(channel);
    run_sub_cli(client).await;
    Ok(())
}

/**
 * 执行子命令
 */
async fn run_sub_cli(mut client: FlightClient) {
    loop {
        print!("> ");
        io::stdout().flush().unwrap(); // 确保提示符被打印
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        let args: Vec<&str> = input.trim().split_whitespace().collect();
        // 如果用户只按了回车，跳过本次循环
        if args.is_empty() {
            continue;
        }
        // 如果用户输入exit，退出
        if args[0] == "exit" {
            break;
        }
        
        // 执行子命令
        match parse_subcmd(args[0],input.clone()) {
            Ok(sub_cmd) => sub_cmd.execute(&mut client).await,
            Err(e) => println!("{}", e),
        }
    }
}


/**
 * 解析子命令
 */
fn parse_subcmd(cmd_type: &str, sub_cmd: String) -> Result<SubCmd,String> {
    match cmd_type {
        // 对应handshake
        "handshake" => {
            let handshake = Handshake::try_parse_from(sub_cmd.split_whitespace());
            match handshake {
                Ok(h) => Ok(SubCmd::Handshake(h)),
                Err(_e) => Err("解析Handshake错误".to_string()),
            }
        }
        // 对应 list_flight
        "flights" => {
            Err("".to_string())
        }
        // 对应 get_flight_info、poll_flight_info
        "flight_info" => {
            // get poll 两种方式
            Err("".to_string())
        }
        // 对应get_schema
        "schema" => {
            let get_schema = GetSchema::try_parse_from(sub_cmd.split_whitespace());
            match get_schema {
                Ok(h) => Ok(SubCmd::GetSchema(h)),
                Err(_e) => Err("解析GetSchema错误".to_string()),
            }
        }
        // 对应 do_get
        "get" => {
            Err("".to_string())
        }
        // 对应 do_put
        "put" => {
            Err("".to_string())
        }
        // 对应 do_action
        "action" => {
            Err("".to_string())
        }
        // 对应 list_actions
        "list_actions" => {
            Err("".to_string())
        }
        // 对应 do_exchange
        "exchange" => {
            Err("".to_string())
        }
        _ => Err(format!("未知命令：{}", cmd_type)),
    }
}

