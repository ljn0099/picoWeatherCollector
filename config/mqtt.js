const mqtt = require("mqtt");
const fs = require("fs");

/**
 * MQTT client configuration and setup
 */
const protocol = "mqtts";
const host = process.env.MQTT_BROKER_HOST;
const port = process.env.MQTT_BROKER_PORT;
const clientId = `mqtt_${Math.random().toString(16).slice(3)}`;

const connectUrl = `${protocol}://${host}:${port}`;

const client = mqtt.connect(connectUrl, {
    clientId,
    clean: true,
    connectTimeout: 4000,
    username: process.env.MQTT_USER,
    password: process.env.MQTT_PASS,
    reconnectPeriod: 5000,
    rejectUnauthorized: true,
    ca: fs.readFileSync("./ca.crt"),
    key: fs.readFileSync("./client.key"),
    cert: fs.readFileSync("./client.crt"),
});

module.exports = client;
