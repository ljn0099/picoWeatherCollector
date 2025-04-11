require("dotenv").config();
const MQTTHandler = require("./services/mqttHandler");

/**
 * Main application entry point
 */
async function main() {
    try {
        console.log("Starting weather station monitoring service...");
        await MQTTHandler.subscribeToStations();
    } catch (err) {
        console.error("Application startup failed:", err);
        process.exit(1);
    }
}

// Start the application
main();
