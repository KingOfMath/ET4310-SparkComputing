import "./index.css";
import chroma from "chroma-js";
import * as L from "leaflet";

const map = L.map("map").setView([0, 0], 2);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution:
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  noWrap: true,
}).addTo(map);
const group = new L.LayerGroup().addTo(map);

const color = chroma
  .scale([
    "#4470b1",
    "#3b92b8",
    "#59b4aa",
    "#7ecba4",
    "#a6dba4",
    "#cae99d",
    "#e8f69c",
    "#f7fcb3",
    "#fef5af",
    "#fee391",
    "#fdc877",
    "#fcaa5f",
    "#f7834d",
    "#ec6145",
    "#da464c",
    "#be2449",
  ])
  .mode("lch");

const defaultStyle = {
  weight: 1,
  opacity: 0.8,
  fillOpacity: 0.3,
};

const style = (size) => ({
  radius: 7 + size * 6,
  color: color(size),
  fillColor: color(size),
  ...defaultStyle,
});

const layers = new Map();
const counts = new Map();
const updates = new EventSource("updates");

updates.onmessage = (event) => {
  const { count, feature } = JSON.parse(event.data);
  const id = feature.properties.geoname_id;

  counts.set(id, count);
  const max = Math.log(Math.max(...counts.values()));
  const scale = (count) => Math.log(count) / max;

  if (layers.has(id)) {
    layers.get(id).remove();
    layers.delete(id);
  }

  layers.forEach((layer, id) => {
    group
      .getLayer(group.getLayerId(layer))
      .setStyle(style(scale(counts.get(id))));
  });

  if (count > 0) {
    const text = `${feature.properties.name}: ${count}`;
    const layer = L.geoJSON(feature, {
      pointToLayer: (_, latlng) =>
        L.circleMarker(latlng, style(scale(count)))
          .bindPopup(text)
          .bindTooltip(text),
    });

    layers.set(id, layer);
    group.addLayer(layer);
  }
};
