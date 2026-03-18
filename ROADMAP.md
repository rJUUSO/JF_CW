JUUSOFLUX_COWORKER Roadmap

Phase 0: Foundation

- [x] Create PlatformIO project
- [x] Define coworker TOML config
- [x] Define transport-neutral command protocol
- [x] Scaffold RS485 and CAN link backends

Phase 1: Bring-up

- [x] Verify RS485 command forwarding through JUUSOFLUX_HUB
- [ ] Verify CAN segmented text transport through JUUSOFLUX_HUB
- [x] Add coworker heartbeat debug line (`@DBG=ALIVE!`)
- [ ] Add stale-link timeout behavior
- [ ] Add hub-side link power select (`PWR485` / `PWRCAN`)

Phase 2: Remote service interface

- [x] HUB forwards host commands to coworker
- [x] HUB forwards coworker replies to host
- [ ] HUB forwards coworker debug stream to host
- [x] Support `@CFG=GET`, `@CFG=SET`, `@CFG=SAVE`, `@CFG=RELOAD` remotely
- [x] Add addressed RS485 envelope handling (`@>addr:...` -> `@<addr:...`)

Phase 3: Sensor acquisition

- [x] GPS fix parsing and snapshot reporting
- [x] MQTT analyzer ingestion and field cache
- [x] SHT4X measurement support
- [x] BMP280 measurement support

Phase 4: Poll-all integration

- [x] Make `@POLL=ALL!` return a complete merged snapshot
- [x] Include analyzer MQTT summary
- [x] Include GPS fix/time/location summary
- [x] Include I2C sensor readings and presence bits
- [ ] Include explicit link health and config revision/CRC fields

Phase 5: Reliability

- [ ] Add transport ACK/NACK and retry rules
- [x] Add sequence-gap detection (CAN RX mismatch counter)
- [ ] Add config CRC/versioning
- [ ] Add watchdog-safe reconnect behavior
- [ ] Add optional local ring buffer when HUB link is down

Phase 6: HUB integration

- [x] Add coworker transport bridge to JUUSOFLUX_HUB
- [x] Add coworker status command on HUB
- [x] Add coworker config proxy on HUB
- [ ] Add merged logging from HUB local sensors + coworker sensors (partially implemented)
