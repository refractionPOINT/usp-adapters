# LimaCharlie Adapter + Zeek

This is a simple container running Zeek with a direct feed into LimaCharlie
which allows you to quickly deploy Zeek sensors and get retention, alerting
and forwarding in the cloud.

This container is available on Docker Hub as `refractionpoint/lc-adapter-zeek`.

## Usage

User needs to define the following environment variables:
* `OID` = The LimaCharlie Organization ID
* `IKEY` = The LimaCharlie Installation Key
* `NAME` = A unique name for the sensor
* `INGESTION_TOKEN` = The LimaCharlie Ingestion Key

Optional environment variables:
* `IFACE` = The interface to listen on (default: eth0)
* `ZEEK_ARGS` = Additional arguments to pass to Zeek

Example command line with Docker:
```bash
docker run -it -e OID=aaaaaaaa-bfa1-bbbb-cccc-138cd51389cd -e IKEY=aaaaaaaa-9ae6-bbbb-cccc-5e42b854adf5 -e INGESTION_TOKEN=aaaaaaaa-9ae6-bbbb-dddd-5e42b854adf5 -e NAME=zeek -e IFACE=eth0 refractionpoint/lc-adapter-zeek
```