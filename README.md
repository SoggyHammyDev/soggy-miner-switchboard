# Soggy Miner Switchboard

Umbrel-ready Stratum V1 switchboard for switching ASIC miners between upstream pools without rebooting the miners.

## How the MVP works

1. Point each miner to `stratum+tcp://<umbrel-ip>:3338` once.
2. The Switchboard identifies the miner by source LAN IP.
3. Assign that IP to a saved upstream pool in the dashboard.
4. Changing the assignment closes only the miner's Stratum TCP session. The ASIC reconnects to the same Switchboard endpoint and is immediately routed to the new upstream.

This avoids changing AxeOS/firmware pool settings for every switch.

## Run locally

```bash
docker build -t soggy-miner-switchboard:dev .
docker run --rm -p 8080:8080 -p 3338:3338 -v "$PWD/data:/data" soggy-miner-switchboard:dev
```

Open `http://localhost:8080`.

## MVP limitations

- Stratum V1 TCP only.
- Routing is by miner source IP; static DHCP leases are recommended.
- Miner must reconnect after a route change. The ASIC does not reboot.
- TLS Stratum and DNS failover are future work.
