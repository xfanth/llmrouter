# Smoke fixture config

Mounted at `/config` by `docker-compose.ci.yml`. Deliberately empty of
providers: `Config::load` tolerates every file being missing, so the router
boots with defaults, no channels and no outbound calls. That is all the smoke
tier needs to prove the image starts and serves.

Never add real key material here. If a case ever needs a key, use an obvious
fake such as `sk-test-not-a-real-key`.
