const { DateTime } = require("luxon");
const DBService = require("./dbService");
const client = require("../config/mqtt");

/**
 * Handles MQTT message processing and station subscriptions
 */
class MQTTHandler {
    /**
     * Process incoming MQTT messages
     * @param {string} stationId - ID of the weather station
     * @param {string} message - Raw message payload
     */

    static formatDate(dateStr) {
        const dt = DateTime.fromFormat(dateStr, "EEEE d MMMM H:mm:ss yyyy", {
            zone: "UTC",
        });

        if (!dt.isValid) {
            throw new Error(
                `Invalid UTC date: ${dateStr}. Error: ${dt.invalidExplanation}`,
            );
        }

        return dt.toUTC().toISO({ suppressMilliseconds: true });
    }

    static async handleStationMessage(stationId, message) {
        try {
            const rawData = JSON.parse(message);
            const availableFields =
                await DBService.getAvailableFields(stationId);

            // Validate date first (special case)
            if (!rawData.date) {
                throw new Error("Missing mandatory field: date");
            }

            // Validate all available fields are present
            const missingFields = [];
            const processedData = {
                station_id: stationId,
                date: this.formatDate(rawData.date),
            };

            // Check each available field
            for (const [field, isAvailable] of Object.entries(
                availableFields,
            )) {
                if (isAvailable) {
                    if (rawData[field] === undefined) {
                        missingFields.push(field);
                    } else {
                        processedData[field] = rawData[field];
                    }
                }
            }

            if (missingFields.length > 0) {
                throw new Error(
                    `Missing data for available fields: ${missingFields.join(", ")}`,
                );
            }

            // Only proceed if all fields are present
            await DBService.insertWeatherData(processedData);
            await DBService.calculateHourlyStats(stationId, processedData.date);
            await DBService.calculateDailyStats(stationId, processedData.date);
        } catch (err) {
            console.error(`[Station ${stationId}] Invalid data:`, err.message);
        }
    }
    /**
     * Subscribe to all weather station topics
     */
    static async subscribeToStations() {
        try {
            const stations = await DBService.getAllStationIds();

            client.on("connect", () => {
                console.log("Connected to MQTT broker");

                stations.forEach((station) => {
                    const topic = `/${station.station_id}`;
                    client.subscribe(topic, (err) => {
                        if (err) {
                            console.error(
                                `Error subscribing to topic ${topic}:`,
                                err,
                            );
                        } else {
                            console.log(`Subscribed to topic '${topic}'`);
                        }
                    });
                });
            });

            client.on("message", (topic, payload) => {
                try {
                    const stationId = topic.substring(1);
                    const message = payload.toString();
                    this.handleStationMessage(stationId, message);
                } catch (err) {
                    console.error("Error processing message:", err);
                }
            });

            client.on("error", (err) => {
                console.error("MQTT connection error:", err);
            });
        } catch (err) {
            console.error("Failed to subscribe to stations:", err);
        }
    }
}

module.exports = MQTTHandler;
