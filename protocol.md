JUUSOFLUX_COWORKER Link Protocol

Goals

- use the same host-visible command model over RS485 or CAN
- allow HUB to act as a transparent proxy between host and coworker
- support remote config editing, debug forwarding, and poll-all requests

Application layer

- line-oriented ASCII commands ending with `!`
- examples:
	- `@SYS=HELLO!`
	- `@CAN=STATUS!`
	- `@LINK=STATUS!`
	- `@CFG=GET:wifi.ssid!`
	- `@CFG=SET:link.mode=can!`
	- `@POLL=ALL!`

Response style

- reply lines follow HUB style where practical
- examples:
	- `@SYS=OK:JUUSOFLUX_COWORKER!`
	- `@BOOT=OK:JUUSOFLUX_COWORKER!`
	- `@CAN=STATUS:MODE=can,READY=1,...!`
	- `@CFG=link.mode=can!`
	- `@MQTT=RX:JSON={"licor":{"niobrara":{"output":{"concentration":{...}}}}}!`
	- `@MQTT=RX:TOPIC=licor/niobrara/output/concentration,NAME=LI7820,JSON={...}!`
	- `@POLL=ALL:MQTT_age_ms=914,GPS_fix=0,GPS_sats=0,GPS_latitude=-,GPS_longitude=-,GPS_time=2026-03-17T15:06:02Z,SHT4X_44_temp_c=25.43,SHT4X_44_rh_pct=32.50,BMP280_76_temp_c=25.90,BMP280_76_press_pa=102069.8,LI7820_seconds=1773771240.166730,LI7820_nanoseconds=0,LI7820_ndx=508.926971,LI7820_diag=7074.569824,LI7820_h2o=346.006012,LI7820_n2o_0=39.852798,LI7820_n2o_1=1,LI7820_residual_0=55.001041,LI7820_residual_1=1,LI7820_time_0=30.575748,LI7820_time_1=1,LI7820_t_0=32.719879,LI7820_t_1=1,LI7820_error=0,LI7820_shift=0.011371,LI7820_voltage=0.006095,LI7820_a12=0.017466,LI7820_a13=0.000000,LI7820_a14=0.000000!`

Implemented command groups

- system: `@SYS=HELLO!`
- CAN: `@CAN=STATUS!`, `@CAN=REINIT!`, `@CAN=TXTEST!`
- link: `@LINK=STATUS!`, `@LINK=ADDR!`
- Wi-Fi/MQTT: `@WIFI=STATUS!`, `@MQTT=STATUS!`, `@MQTT=LAST!`, `@MQTT=READ!`, `@MQTT=READ:<topic>!`
- GPS: `@GPS=STATUS!`, `@GPS=RAW!`
- I2C: `@I2C=SCAN!`, `@I2C=LIST!`, `@I2C=SHT4X:READ!`, `@I2C=BMP280:READ!`
- aggregate: `@POLL=ALL!`
- config: `@CFG=GET:<key>!`, `@CFG=SET:<key>=<value>!`, `@CFG=SAVE!`, `@CFG=RELOAD!`
- debug: `@DBG=ENABLE!`, `@DBG=DISABLE!`

MQTT read behavior

- `@MQTT=READ!` returns cached JSON root snapshot as `@MQTT=RX:JSON={...}!`
- `@MQTT=READ:<topic>!` returns cached JSON for that path as `@MQTT=RX:TOPIC=<topic>,NAME=<mqtt.name>,JSON={...}!`
- topic read falls back to `<topic>/data` path when present
- topic read is allow-listed by `mqtt.cache_topics`; out-of-scope topics return `@MQTT=ERR:NO_TOPIC_DATA!`
- `@MQTT=LAST!` returns latest topic/payload with age as an operational debug view

Debug stream

- coworker emits debug/status lines as normal ASCII lines
- HUB can forward them to host unchanged
- examples:
	- `@DBG=MQTT:CONNECTED!`
	- `@DBG=GPS:FIX!`

RS485 transport

- physical layer: half-duplex RS485 using an auto-direction adapter or explicit DE/RE control
- payload: raw ASCII command lines terminated with `\n`
- intended first bring-up mode because it is easy to observe and debug
- optional addressed envelope can be used: `@><node_id>:<command>`; replies are prefixed `@<<node_id>:` for matched addresses

CAN transport

- physical layer: classic CAN 2.0 via ESP32 TWAI + external transceiver
- command and response lines are segmented into small CAN payload chunks

Default CAN IDs

- `0x501`: HUB -> COWORKER command frames
- `0x502`: COWORKER -> HUB response frames
- `0x503`: COWORKER -> HUB debug frames

CAN frame payload layout

- `byte0`: sequence number
- `byte1`: flags
	- bit0: start-of-message
	- bit1: end-of-message
- `byte2..7`: ASCII payload chunk, up to 6 bytes

CAN reassembly rules

- receiver clears current buffer on start-of-message
- chunks with mismatched sequence are dropped
- completed message is accepted only when end-of-message is received
- mismatch statistics are exposed through `@CAN=STATUS!` counters (`RX_SEQ_ERR`, `RX_ADDR_REJ`)

Remote config workflow

1. Host sends `@CFG=SET:key=value!` to HUB.
2. HUB forwards line to coworker over selected link.
3. Coworker updates its in-memory config and replies `@CFG=OK:key=value!` or `@CFG=ERR:key!`.
4. Host sends `@CFG=SAVE!`.
5. Coworker writes `coworker.toml` to LittleFS.

Poll-all workflow

1. Host sends `@POLL=ALL!`.
2. HUB forwards command to coworker.
3. Coworker replies with one compact line containing:
	- MQTT cache age
	- GPS fix/satellite/location/time fields only when `[gps].enabled = 1`
	- sensor readings keyed by sensor address (`SHT4X_<addr>_*`, `BMP280_<addr>_*`) only for addresses listed in `[logging]`
	- dynamic MQTT payload-derived fields (for example `LI7820_<field>=...`)
	- keyed MQTT arrays are flattened to their primary value, so a payload field like `H2O:[40.1,1]` becomes `LI7810_h2o=40.1`

Extensibility

- keep command grammar ASCII for serviceability
- if binary telemetry is needed later, add a second message class while preserving the existing text control plane

Known gaps

- no link-layer ACK/NACK retry handshake yet
- no config revision/CRC field in responses yet
- no watchdog-driven link recovery state machine yet
