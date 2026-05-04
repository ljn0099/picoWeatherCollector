# Authentication

- **Username:** `<station_uuid>`  
- **Password:** `<api_key>`

---

# MQTT Topics

Weather data from each weather station should be published to the following topic:

The station can publish in all topics that have the `stations/<station_uuid>/` prefix. Where:

- **stations**: static prefix for all weather stations.  
- **`<station_uuid>`**: unique UUIDv4 of the station (matches the username used for authentication).  

**Note:** Don't use a leading `/` at the beginning of the topic.

## Examples

| Station UUID                         | Topic                                                      |
|--------------------------------------|------------------------------------------------------------|
| 123e4567-e89b-12d3-a456-426614174000 | stations/123e4567-e89b-12d3-a456-426614174000/data         |
| 987f6543-e21b-34d5-b678-123456789abc | stations/987f6543-e21b-34d5-b678-123456789abc/reboot       |

**Note**: The `stations/<station_uuid>/data` is the only endpoint currently managed by the collector
from the time writting this, the others endpoints can only be used manually at the moment.

## Ping/uptime topics

The station is suscribed to `stations/<station_uuid>/ping` and when it recieves a message it must
answer with his current uptime in seconds on `stations/<station_uuid>/uptime`.

## Reboot topic

The station is suscribed to `stations/<station_uuid>/reboot` and when it recieves a message it must
reboot itself.

## Will topic

The stations sets its will topic to `stations/<station_uuid>/online` configuring its will msg to `0`, sets the retain flag
and finally publishes the message `1` to it indicating that it is active.

---

## Weather data topic

`stations/<station_uuid>/data`

The current weather.proto file is:
```
syntax = "proto3";

package weather;

import "google/protobuf/wrappers.proto";

message WeatherMeasurement {
  // Mandatory fields
  uint64 periodStart = 1;
  uint64 periodEnd = 2;

  // Optional fields
  google.protobuf.FloatValue temperature = 4;
  google.protobuf.FloatValue humidity = 5;
  google.protobuf.FloatValue pressure = 6;
  google.protobuf.FloatValue lux = 7;
  google.protobuf.FloatValue uvi = 8;
  google.protobuf.FloatValue windSpeed = 9;
  google.protobuf.FloatValue windDirection = 10;
  google.protobuf.FloatValue gustSpeed = 11;
  google.protobuf.FloatValue gustDirection = 12;
  google.protobuf.FloatValue rainfall = 13;
  google.protobuf.FloatValue solarIrradiance = 14;
}
```

Where:

**Mandatory fields**
- **`periodStart`**: Timestamp in seconds since the Unix epoch (1970-01-01T00:00:00Z) when the station started the measurement.
- **`periodEnd`**: Timestamp in seconds since the Unix epoch (1970-01-01T00:00:00Z) when the station finished the measurement.

**Optional fields**

The next fields are represented with google.protobuf.FloatValue for being able to identify if the data was sent or not.
- **`temperature`**: Temperature in Celsius degrees.
- **`humidity`**: Relative humidity in percentaje
- **`pressure`**: Pressure in hPa.
- **`lux`**: Illuminance in lux.
- **`uvi`**: UVI index.
- **`windSpeed`**: Average wind speed during the measurement in km/h.
- **`windDirection`**: Average(circular mean) wind direction during the measurement in degrees [0-360)º.
- **`gustSpeed`**: Maximun wind speed in a small period of time (ej. 2 sec) during the measurement in km/h.
- **`gustDirection`**: Wind direction that corresponds with the gust speed in degrees [0-360)º.
- **`rainfall`**: Total precipitation during the measurement in mm.
- **`solarIrradiance`**: Solar irradiance during the measurement in W/m^2.

It is recomeded to send the weather in periods of 10-20 minutes for better resolution, and
to average all the paramenters (except rainfall, gust speed and gust direction) during the measurement
for better accuracy.

# The code

It is used as a mosquitto plugin using the plugin api version 5.

It communicates with the picoWeatherDB using libpq and authenticates the stations hashing the api key password and
checking it against the database for that station.

The decoding of the messagese is done by the [nanobp](github.com/nanopb/nanopb) library, and it uses a pool of threads
and a message queue for not blocking the main mosquitto thread, it also uses a pool of psql connections to the database
for simultaneous access and not having to create the connection every time.

The deployement is made with docker, the default config is:
```
plugin /usr/lib/mosquitto/plugins/picoWeatherCollector.so

plugin_opt_db_host localhost
plugin_opt_db_user weatherCollector
plugin_opt_db_pass weatherCollector
plugin_opt_db_name weather
plugin_opt_db_port 5432
plugin_opt_max_db_conn 4
plugin_opt_num_threads 4

persistence true
persistence_location /mosquitto/data/
log_dest file /mosquitto/log/mosquitto.log
listener 1883
```

Where:
`plugin /usr/lib/mosquitto/plugins/picoWeatherCollector.so` is the location where the plugin was installed by the Dockerfile
`plugin_opt_db_host <host>` host where the database is.
`plugin_opt_db_user <user>` user for the database
`plugin_opt_db_pass <pass>` password for the database
`plugin_opt_db_name <name>` database name
`plugin_opt_db_port <port>` database port
`plugin_opt_max_db_conn <number>` number of threads to create for the database pool
`plugin_opt_num_threads <number>` number of threads to create for the message decoding

# Project server

The project hosts a mqtt server on mqtt.picoweather.net, to use it you first need to create an account
on api.picoweather.net and send me an email for activating your account to create weather station.

The servers uses MQTTs over the 8883 port using TLS1.3 exclusively.
