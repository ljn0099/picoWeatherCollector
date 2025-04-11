const pool = require("../config/db");

/**
 * Database service layer for weather station operations
 */
class DBService {
    /**
     * Get all weather station IDs from database
     * @returns {Promise<Array<{station_id: number}>>} Array of station IDs
     */
    static async getAllStationIds() {
        try {
            const res = await pool.query(
                "SELECT station_id FROM weather_stations",
            );
            return res.rows;
        } catch (err) {
            console.error("Database error:", err);
            throw err;
        }
    }

    /**
     * Get available fields for a specific weather station
     * @param {number} stationId - The ID of the weather station
     * @returns {Promise<Object>} Object with available fields as boolean flags
     */
    static async getAvailableFields(stationId) {
        try {
            const query = `
        SELECT 
          temperature, pressure, humidity, lux, uvi, 
          rain_gauge, anemometer, wind_vane
        FROM weather_stations
        WHERE station_id = $1
      `;

            const res = await pool.query(query, [stationId]);

            if (res.rows.length === 0) {
                throw new Error(`Station with ID ${stationId} not found`);
            }

            // Map the database columns to weather_data fields
            const stationConfig = res.rows[0];
            return {
                temperature: stationConfig.temperature,
                pressure: stationConfig.pressure,
                humidity: stationConfig.humidity,
                lux: stationConfig.lux,
                uvi: stationConfig.uvi,
                rain: stationConfig.rain_gauge,
                wind_speed: stationConfig.anemometer,
                wind_direction: stationConfig.wind_vane,
                // Gust is typically derived from anemometer data
                gust_speed: stationConfig.anemometer,
                gust_direction:
                    stationConfig.anemometer && stationConfig.wind_vane,
            };
        } catch (err) {
            console.error("Error getting available fields:", err);
            throw err;
        }
    }
    /**
     * Insert weather data into the database
     * @param {Object} data - Weather data object
     * @returns {Promise<void>}
     */
    static async insertWeatherData(data) {
        try {
            // Build the query dynamically based on available fields
            const fields = Object.keys(data);
            const values = Object.values(data);

            const placeholders = fields.map((_, i) => `$${i + 1}`).join(", ");
            const columns = fields.join(", ");

            const query = `
        INSERT INTO weather_data (${columns})
        VALUES (${placeholders})
        ON CONFLICT (station_id, date) DO UPDATE
        SET ${fields.map((f) => `${f} = EXCLUDED.${f}`).join(", ")}
      `;

            await pool.query(query, values);
            console.log(
                `Data inserted for station ${data.station_id} at ${data.date}`,
            );
        } catch (err) {
            console.error("Error inserting weather data:", err);
            throw err;
        }
    }
    /**
     * Calculate hourly aggregates
     * @param {number} stationId
     * @param {string} dateTime ISO timestamp
     */
    static async calculateHourlyStats(stationId, dateTime) {
        const query = `
    WITH gust_data AS (
      SELECT gust_speed, gust_direction
      FROM weather_data
      WHERE station_id = $1
        AND date >= date_trunc('hour', $2::timestamptz)
        AND date < date_trunc('hour', $2::timestamptz) + interval '1 hour'
        AND gust_speed IS NOT NULL
    ),
    hourly_stats AS (
      SELECT
        date_trunc('hour', $2::timestamptz) AS hour,

        CASE WHEN COUNT(temperature) > 0 THEN AVG(temperature) ELSE NULL END AS avg_temperature,
        CASE WHEN COUNT(humidity) > 0 THEN AVG(humidity) ELSE NULL END AS avg_humidity,
        CASE WHEN COUNT(pressure) > 0 THEN AVG(pressure) ELSE NULL END AS avg_pressure,
        CASE WHEN COUNT(rain) > 0 THEN SUM(rain) ELSE NULL END AS sum_rain,
        CASE WHEN COUNT(wind_speed) > 0 THEN AVG(wind_speed) ELSE NULL END AS avg_wind_speed,
        CASE WHEN COUNT(wind_speed) > 0 THEN STDDEV_POP(wind_speed) ELSE NULL END AS standard_deviation_speed,
        CASE WHEN COUNT(wind_direction) > 0 THEN
          (360 + DEGREES(ATAN2(
            AVG(SIN(RADIANS(wind_direction))),
            AVG(COS(RADIANS(wind_direction)))
          ))::NUMERIC % 360)
        ELSE NULL END AS avg_wind_direction,
        CASE WHEN COUNT(lux) > 0 THEN AVG(lux) ELSE NULL END AS avg_lux,
        CASE WHEN COUNT(uvi) > 0 THEN AVG(uvi) ELSE NULL END AS avg_uvi,

        (SELECT gust_speed FROM gust_data ORDER BY gust_speed DESC LIMIT 1) AS max_gust_speed,
        (SELECT gust_direction FROM gust_data ORDER BY gust_speed DESC LIMIT 1) AS max_gust_direction

      FROM weather_data
      WHERE station_id = $1
        AND date >= date_trunc('hour', $2::timestamptz)
        AND date < date_trunc('hour', $2::timestamptz) + interval '1 hour'
    )
    INSERT INTO weather_hourly (
      station_id,
      date,
      avg_temperature,
      avg_humidity,
      avg_pressure,
      sum_rain,
      avg_wind_speed,
      standard_deviation_speed,
      avg_wind_direction,
      avg_lux,
      avg_uvi,
      max_gust_speed,
      max_gust_direction
    )
    SELECT
      $1,
      hour,
      avg_temperature,
      avg_humidity,
      avg_pressure,
      sum_rain,
      avg_wind_speed,
      standard_deviation_speed,
      avg_wind_direction,
      avg_lux,
      avg_uvi,
      max_gust_speed,
      max_gust_direction
    FROM hourly_stats
    ON CONFLICT (station_id, date) DO UPDATE SET
      avg_temperature = EXCLUDED.avg_temperature,
      avg_humidity = EXCLUDED.avg_humidity,
      avg_pressure = EXCLUDED.avg_pressure,
      sum_rain = EXCLUDED.sum_rain,
      avg_wind_speed = EXCLUDED.avg_wind_speed,
      standard_deviation_speed = EXCLUDED.standard_deviation_speed,
      avg_wind_direction = EXCLUDED.avg_wind_direction,
      avg_lux = EXCLUDED.avg_lux,
      avg_uvi = EXCLUDED.avg_uvi,
      max_gust_speed = EXCLUDED.max_gust_speed,
      max_gust_direction = EXCLUDED.max_gust_direction;
  `;

        try {
            await pool.query(query, [stationId, dateTime]);
        } catch (err) {
            console.error("Hourly stats calculation failed:", {
                stationId,
                error: err.message,
            });
            throw err;
        }
    }
    /**
     * Calculate daily aggregates with timezone conversion to Europe/Madrid
     * @param {number} stationId
     * @param {string} date ISO date string (YYYY-MM-DD)
     */
    static async calculateDailyStats(stationId, dateTime) {
        try {
            // Convert the input UTC timestamp to Madrid timezone day boundaries
            const query = `
            WITH madrid_day_range AS (
                SELECT 
                    date_trunc('day', $2::timestamptz AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid' AS day_start_madrid,
                    (date_trunc('day', $2::timestamptz AT TIME ZONE 'Europe/Madrid') + interval '1 day' - interval '1 second') AT TIME ZONE 'Europe/Madrid' AS day_end_madrid
            ),
            day_range AS (
                SELECT 
                    (SELECT day_start_madrid FROM madrid_day_range) AT TIME ZONE 'UTC' AS day_start_utc,
                    (SELECT day_end_madrid FROM madrid_day_range) AT TIME ZONE 'UTC' AS day_end_utc,
                    (SELECT day_start_madrid FROM madrid_day_range)::date AS madrid_date
            ),
            day_data AS (
                SELECT *
                FROM weather_data
                WHERE station_id = $1
                AND date >= (SELECT day_start_utc FROM day_range)
                AND date <= (SELECT day_end_utc FROM day_range)
            ),
            gust_data AS (
                SELECT gust_speed, gust_direction
                FROM day_data
                WHERE gust_speed IS NOT NULL
                ORDER BY gust_speed DESC
                LIMIT 1
            ),
            daily_stats AS (
                SELECT
                    (SELECT madrid_date FROM day_range) AS date,
                    MAX(temperature) FILTER (WHERE temperature IS NOT NULL) AS max_temperature,
                    MIN(temperature) FILTER (WHERE temperature IS NOT NULL) AS min_temperature,
                    MAX(humidity) FILTER (WHERE humidity IS NOT NULL) AS max_humidity,
                    MIN(humidity) FILTER (WHERE humidity IS NOT NULL) AS min_humidity,
                    MAX(pressure) FILTER (WHERE pressure IS NOT NULL) AS max_pressure,
                    MIN(pressure) FILTER (WHERE pressure IS NOT NULL) AS min_pressure,
                    (SELECT gust_speed FROM gust_data) AS max_gust_speed,
                    (SELECT gust_direction FROM gust_data) AS max_gust_direction,
                    CASE WHEN COUNT(wind_speed) > 0 THEN STDDEV_POP(wind_speed) ELSE NULL END AS standard_deviation_speed,
                    CASE WHEN COUNT(wind_speed) > 0 THEN AVG(wind_speed) ELSE NULL END AS avg_wind_speed,
                    CASE WHEN COUNT(wind_direction) > 0 THEN
                        (360 + DEGREES(ATAN2(
                            AVG(SIN(RADIANS(wind_direction))),
                            AVG(COS(RADIANS(wind_direction)))
                        ))::NUMERIC % 360)
                    ELSE NULL END AS avg_wind_direction,
                    MAX(uvi) FILTER (WHERE uvi IS NOT NULL) AS max_uvi,
                    MAX(lux) FILTER (WHERE lux IS NOT NULL) AS max_lux,
                    MIN(lux) FILTER (WHERE lux IS NOT NULL) AS min_lux,
                    SUM(rain) FILTER (WHERE rain IS NOT NULL) AS sum_rain
                FROM day_data
            )
            INSERT INTO weather_daily (
                station_id,
                date,
                max_temperature,
                min_temperature,
                max_humidity,
                min_humidity,
                max_pressure,
                min_pressure,
                max_gust_speed,
                max_gust_direction,
                standard_deviation_speed,
                avg_wind_speed,
                avg_wind_direction,
                max_uvi,
                max_lux,
                min_lux,
                sum_rain
            )
            SELECT
                $1,
                date,
                max_temperature,
                min_temperature,
                max_humidity,
                min_humidity,
                max_pressure,
                min_pressure,
                max_gust_speed,
                max_gust_direction,
                standard_deviation_speed,
                avg_wind_speed,
                avg_wind_direction,
                max_uvi,
                max_lux,
                min_lux,
                sum_rain
            FROM daily_stats
            ON CONFLICT (station_id, date) DO UPDATE SET
                max_temperature = EXCLUDED.max_temperature,
                min_temperature = EXCLUDED.min_temperature,
                max_humidity = EXCLUDED.max_humidity,
                min_humidity = EXCLUDED.min_humidity,
                max_pressure = EXCLUDED.max_pressure,
                min_pressure = EXCLUDED.min_pressure,
                max_gust_speed = EXCLUDED.max_gust_speed,
                max_gust_direction = EXCLUDED.max_gust_direction,
                standard_deviation_speed = EXCLUDED.standard_deviation_speed,
                avg_wind_speed = EXCLUDED.avg_wind_speed,
                avg_wind_direction = EXCLUDED.avg_wind_direction,
                max_uvi = EXCLUDED.max_uvi,
                max_lux = EXCLUDED.max_lux,
                min_lux = EXCLUDED.min_lux,
                sum_rain = EXCLUDED.sum_rain;
        `;

            await pool.query(query, [stationId, dateTime]);
            console.log(
                `Daily stats calculated for station ${stationId} for date in Madrid timezone`,
            );
        } catch (err) {
            console.error("Daily stats calculation failed:", {
                stationId,
                error: err.message,
            });
            throw err;
        }
    }
}

module.exports = DBService;
