use chrono::Utc;
use geojson::GeoJson;
use rand::{prelude::ThreadRng, seq::SliceRandom, thread_rng, Rng};
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{error::Error, fs::File, io::Read, thread::sleep, time::Duration};

const BEER_STYLES: [&'static str; 52] = [
    "Altbier",
    "Amber ale",
    "Barley wine",
    "Berliner Weisse",
    "Bière de Garde",
    "Bitter",
    "Blonde Ale",
    "Bock",
    "Brown ale",
    "California Common/Steam Beer",
    "Cream Ale",
    "Dortmunder Export",
    "Doppelbock",
    "Dunkel",
    "Dunkelweizen",
    "Eisbock",
    "Flanders red ale",
    "Golden/Summer ale",
    "Gose",
    "Gueuze",
    "Hefeweizen",
    "Helles",
    "India pale ale",
    "Kölsch",
    "Lambic",
    "Light ale",
    "Maibock/Helles bock",
    "Malt liquor",
    "Mild",
    "Oktoberfestbier/Märzenbier",
    "Old ale",
    "Oud bruin",
    "Pale ale",
    "Pilsener/Pilsner/Pils",
    "Porter",
    "Red ale",
    "Roggenbier",
    "Saison",
    "Stout",
    "Schwarzbier",
    "Vienna lager",
    "Witbier",
    "Weissbier",
    "Weizenbock",
    "Fruit beer",
    "Herb and Spice Beer",
    "Honey beer",
    "Rye Beer",
    "Smoked beer",
    "Vegetable beer",
    "Wild beer",
    "Wood-aged beer",
];

pub struct City<'a> {
    id: u64,
    name: &'a str,
    population: u64,
}

#[derive(Serialize)]
pub struct Event {
    timestamp: u64,
    city_id: u64,
    city_name: String,
    style: String,
}

impl Event {
    fn random(timestamp: u64, cities: &Vec<City>, mut rng: &mut ThreadRng) -> Self {
        let city = cities
            .choose_weighted(&mut rng, |city| city.population)
            .unwrap();
        Event {
            timestamp,
            city_id: city.id,
            city_name: city.name.to_string(),
            style: BEER_STYLES.choose(&mut rng).unwrap().to_string(),
        }
    }
}

fn geojson_properties<'a>(input: &'a GeoJson) -> impl Iterator<Item = &'a Map<String, Value>> {
    match input {
        GeoJson::FeatureCollection(ref collection) => collection
            .features
            .iter()
            .filter_map(|feature| feature.properties.as_ref()),
        _ => panic!("bad file"),
    }
}

fn read_cities<'a>(input: &'a GeoJson) -> Vec<City> {
    geojson_properties(input)
        .filter_map(|properties| {
            properties.get("geoname_id").and_then(|id| {
                properties.get("name").and_then(|name| {
                    properties
                        .get("population")
                        .and_then(|population| Some((id, name, population)))
                })
            })
        })
        .filter_map(|(id, name, population)| {
            id.as_str().and_then(|id| {
                name.as_str().and_then(|name| {
                    population.as_u64().and_then(|population| {
                        Some(City {
                            id: id.parse::<u64>().unwrap(),
                            name,
                            population: population * (population as f64).sqrt() as u64,
                        })
                    })
                })
            })
        })
        .filter(|city| city.population > 100_000u64)
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut contents = String::new();
    File::open("/src/cities.geojson")
        .expect("failed to open file")
        .read_to_string(&mut contents)
        .expect("failed to read file");
    let geojson = contents
        .parse::<GeoJson>()
        .expect("failed to parse geojson");
    let cities = read_cities(&geojson);

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", "kafka-server:9092")
        .create()?;

    let mut rng = thread_rng();

    loop {
        let event = Event::random(Utc::now().timestamp_millis() as u64, &cities, &mut rng);
        producer
            .send(
                FutureRecord::to("events")
                    .payload(&serde_json::to_string(&event).unwrap())
                    .key(&event.city_id.to_string()),
                Duration::from_secs(0),
            )
            .await
            .map_err(|e| e.0)?;

        sleep(Duration::from_millis(rng.gen_range(100, 500)));
    }
}
