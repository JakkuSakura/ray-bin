use serde::{Deserialize, Serialize};
use std::env;
use clickhouse::{Client, Row};
// use std::any::type_name;
use tokio;

// fn type_of<T>(_: T) -> &'static str {
//     type_name::<T>()
// }

#[derive(Row,Debug, Deserialize, Serialize)]
struct TickData {
    event_st: i64,
    transact_st: i64,
    receive_lt: i64,
    instrument: String,
    ap1: f64,
    aq1: f64,
    bp1: f64,
    bq1: f64
}

#[derive(Debug, Serialize)]
struct OutputSMA {
    instrument: String,
    event_st: i64,
    sma: Option<f64>,
}

// fn calculate_sma(data:Vec<f64>,window_size:usize)->Vec<Option<f64>>{
//     data.windows(window_size)
//     .map(|window|Some(window.iter().sum::<f64>()/window_size as f64))
//     .collect::<Vec<_>>()
//     .into_iter()
//     .chain(vec![None;window_size-1])
//     .collect()
// }

fn calculate_sma(data: Vec<f64>, window_size: usize) -> Vec<Option<f64>> {
    
    let mut sma = vec![None; window_size - 1];

    sma.extend(
        data.windows(window_size)
            .map(|window| Some(window.iter().sum::<f64>() / window_size as f64))
    );

    sma
}

#[tokio::main]
async fn main() ->Result<(), Box<dyn std::error::Error>>{
    let args: Vec<String> = env::args().collect();
    let start_time = &args[1];
    let end_time = &args[2];

    // if args.len() != 3 { 
    //     eprintln!("Usage: {} <start_time> <end_time>", args[0]); 
    //     std::process::exit(1); 
    // }

    let client=Client::default().with_url("http://localhost:8123")
                                        .with_database("default");
                                        // .with_option("async_insert", "1")
                                        // .with_option("wait_for_async_insert", "0");

    let query="select * from market_event_bbo_lite where instrument = 'S:BinanceFutures:BTCUSDT' and event_st between ? and ? limit 500";

    let data=client.query(query).bind(start_time).bind(end_time).fetch_all::<TickData>().await?;
    
    // println!("{:?}",&data);

    let average_price:Vec<f64>=data.iter().map(|row|(row.ap1+row.bp1)/2.0).collect();
    let sma=calculate_sma(average_price, 3);
    
    // for (i,value) in sma.iter().enumerate(){
    //     match value{
    //         Some(v)=>println!("SMA[{}]={}",i,v),
    //         None=>println!("SMA[{}]=None",i),
    //     }
    // }

//     let create_table_query = r#"
//         CREATE TABLE IF NOT EXISTS output_sma_002 (
//             instrument String,
//             event_st UInt64,
//             sma Nullable(Float64)
//         ) ENGINE = MergeTree() ORDER BY event_st;
//      "#;
//     client.query(create_table_query).execute().await?;

    let sma_data: Vec<OutputSMA> = data.iter() 
        .zip(sma.iter()) 
        .map(|(row, sma_value)| OutputSMA { 
            instrument: row.instrument.clone(), 
            event_st: row.event_st.clone(), 
            sma: *sma_value, 
        }) 
        .collect();
    
    println!("sma_data: {:?}", &sma_data); 
    
    // let insert_query = format!( "INSERT INTO output_sma_002 (instrument, event_st, sma) VALUES {}", 
    // sma_data.iter()
    // .map(|row| { format!( "('{}', '{}', {:?})", row.instrument, row.event_st, row.sma.unwrap_or_default() ) })
    // .collect::<Vec<_>>().join(", ") ); 
    
    // println!("insert_query: {}", insert_query); 
    
    for row in sma_data { 
        let insert_query = "INSERT INTO output_sma_002 (instrument, event_st, sma) VALUES (?, ?, ?)"; 
        client.query(insert_query).bind(&row.instrument).bind(&row.event_st).bind(&row.sma).execute().await?; 
    }

    println!("Success!");

    Ok(())
    
}

