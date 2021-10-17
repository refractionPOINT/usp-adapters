# usp-adapters
USP Adapters for various sources.

## Usage

The adapter can be used to access many different sources and many different event types. The main mechanisms specifying the source and type of events are:

1. Adapter Type: this indicates the technical source of the events, like syslog or S3 buckets.
1. Platform: the platform indicates the type of events that are acquired from that source, like text or carbon_black.


### Examples

#### Syslog
Start the container listening for Syslog. This example uses the syslog from a Debian box.
```
docker run --rm -it -p 4444:4444 usp-adapters syslog port=4444 client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd client_options.platform=text "client_options.mapping.parsing_re=(?P<date>... \d\d \d\d:\d\d:\d\d) (?P<host>.+) (?P<exe>.+?)\[(?P<pid>\d+)\]: (?P<msg>.*)" client_options.sensor_seed_key=testclient1
```

Pipe the syslog from a Debian box to the container.
```
journalctl -f -q | netcat 127.0.0.1 4444
```

#### S3

```
./general s3 client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd client_options.platform=carbon_black client_options.sensor_seed_key=tests3 bucket_name=lc-cb-test access_key=YYYYYYYYYY secret_key=XXXXXXXX  "prefix=events/org_key=NKZFDWEM/"
```

#### Stdin

```
./adapter stdin client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd client_options.platform=text "client_options.mapping.parsing_re=(?P<date>... \d\d \d\d:\d\d:\d\d) (?P<host>.+) (?P<exe>.+?)\[(?P<pid>\d+)\]: (?P<msg>.*)" client_options.sensor_seed_key=testclient3 client_options.mapping.event_type_path=exe
```