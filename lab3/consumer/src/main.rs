use futures::StreamExt;
use geojson::{Feature, GeoJson};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, DefaultConsumerContext, StreamConsumer},
    message::Message,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::Infallible, error::Error, fs::File, io::Read};
use tokio::{sync::broadcast, task};
use warp::Filter;

#[derive(Deserialize)]
struct Update {
    city_id: u64,
    count: u64,
}

#[derive(Clone, Debug, Serialize)]
struct Event {
    count: u64,
    feature: Feature,
}

fn read_cities<'a>(input: &'a GeoJson) -> HashMap<u64, &'a Feature> {
    match input {
        GeoJson::FeatureCollection(ref collection) => collection
            .features
            .iter()
            .map(|feature| {
                (
                    feature
                        .properties
                        .as_ref()
                        .unwrap()
                        .get("geoname_id")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap(),
                    feature,
                )
            })
            .collect(),
        _ => panic!("bad file"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (tx, _) = broadcast::channel(10000);

    let tx2 = tx.clone();

    task::spawn(async move {
        let mut contents = String::new();
        File::open("/src/cities.geojson")
            .expect("failed to open file")
            .read_to_string(&mut contents)
            .expect("failed to read file");
        let geojson = contents
            .parse::<GeoJson>()
            .expect("failed to parse geojson");
        let cities = read_cities(&geojson);

        let consumer: StreamConsumer<_> = ClientConfig::new()
            .set("group.id", "123456")
            .set("bootstrap.servers", "kafka-server:9092")
            .create_with_context(DefaultConsumerContext)
            .expect("failed to create consumer");

        consumer
            .subscribe(&["updates"])
            .expect("failed to subscribe");

        let mut stream = consumer.start();

        while let Some(Ok(message)) = stream.next().await {
            match message.payload_view::<str>() {
                Some(Ok(message)) => match serde_json::from_str::<Update>(message) {
                    Ok(update) => {
                        if let Some(feature) = cities.get(&update.city_id) {
                            let feature = (*feature).clone();
                            match serde_json::to_string(&Event {
                                count: update.count,
                                feature,
                            }) {
                                Ok(event) => {
                                    if tx2.receiver_count() > 0 {
                                        if let Err(e) = tx2.send(event) {
                                            eprintln!("Failed to send: {:?}", e)
                                        }
                                    }
                                }
                                Err(e) => eprintln!("Failed to create JSON string: {}", e),
                            }
                        }
                    }
                    Err(e) => eprintln!("Failed to deserialize update: {}", e),
                },
                _ => eprintln!("bad payload format"),
            }
        }
    });

    let updates = warp::path("updates").and(warp::get()).map(move || {
        let rx = tx.clone().subscribe();
        warp::sse::reply(
            warp::sse::keep_alive().stream(
                rx.into_stream()
                    .map(|msg| -> Result<_, Infallible> { Ok(warp::sse::data(msg.unwrap())) }),
            ),
        )
    });

    warp::serve(
        warp::path("dist")
            .and(warp::fs::dir("/dist"))
            .or(warp::path::end().and(warp::fs::file("/dist/index.html")))
            .or(updates),
    )
    .run(([0, 0, 0, 0], 1234))
    .await;

    Ok(())
}
