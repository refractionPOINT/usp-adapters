# usp-adapters
USP Adapters for various sources.

## Usage

### Syslog
Start the container listening for syslog. This example uses the syslog from a Debian box.
```
docker run --rm -it -p 4444:4444 usp-adapters syslog debug=true syslog.port=4444 syslog.client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d syslog.client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd syslog.client_options.platform=text "syslog.client_options.mapping.parsing_re=(?P<date>... \d\d \d\d:\d\d:\d\d) (?P<host>.+) (?P<exe>.+?)\[(?P<pid>\d+)\]: (?P<msg>.*)" syslog.client_options.sensor_seed_key=testclient1
```

Pipe the syslog from a Debian box to the container.
```
journalctl -f -q | netcat 127.0.0.1 4444
```

### S3

```
./general s3 debug=true s3 s3.client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d s3.client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd s3.client_options.platform=carbon_black s3.client_options.sensor_seed_key=tests3 s3.bucket_name=lc-cb-test s3.access_key=YYYYYYYYYY s3.secret_key=XXXXXXXX  "s3.prefix=events/org_key=NKZFDWEM/"
```

### Stdin

```
./adapter stdin debug=true stdin.client_options.identity.installation_key=e9a3bcdf-efa2-47ae-b6df-579a02f3a54d stdin.client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd stdin.client_options.platform=text "stdin.client_options.mapping.parsing_re=(?P<date>... \d\d \d\d:\d\d:\d\d) (?P<host>.+) (?P<exe>.+?)\[(?P<pid>\d+)\]: (?P<msg>.*)" stdin.client_options.sensor_seed_key=testclient3 stdin.client_options.mapping.event_type_path=exe
```