# usp-adapters
USP Adapters for various sources.

## Usage

### Syslog
Start the container listening for Syslog. This example uses the syslog from a Debian box.
```
docker run --rm -it -p 4444:4444 usp-adapters syslog port=4444 client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd client_options.platform=text "client_options.mapping.parsing_re=(?P<date>... \d\d \d\d:\d\d:\d\d) (?P<host>.+) (?P<exe>.+?)\[(?P<pid>\d+)\]: (?P<msg>.*)" client_options.sensor_seed_key=testclient1
```

Pipe the syslog from a Debian box to the container.
```
journalctl -f -q | netcat 127.0.0.1 4444
```

### S3

```
./general s3 client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd client_options.platform=carbon_black client_options.sensor_seed_key=tests3 bucket_name=lc-cb-test access_key=YYYYYYYYYY secret_key=XXXXXXXX  "prefix=events/org_key=NKZFDWEM/"
```

### Stdin

```
./adapter stdin client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd client_options.platform=text "client_options.mapping.parsing_re=(?P<date>... \d\d \d\d:\d\d:\d\d) (?P<host>.+) (?P<exe>.+?)\[(?P<pid>\d+)\]: (?P<msg>.*)" client_options.sensor_seed_key=testclient3 client_options.mapping.event_type_path=exe
```