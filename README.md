# usp-adapters
USP Adapters for various sources.

## Usage
Start the container listening for syslog. This example uses the syslog from a Debian box.
```
docker run -it -p 4444:4444 usp-adapters syslog debug=true syslog.port=4444 syslog.client_options.identity.ingestion_key=ab688b68-d897-4bd5-8467-6605a8a6f6b5 syslog.client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd syslog.client_options.parse_hint=text "syslog.client_options.format_re=(?P<date>... \d\d \d\d:\d\d:\d\d) (?P<host>.+) (?P<event_type>.+?)\[(?P<pid>\d+)\]: (?P<msg>.*)"
```

Pipe the syslog from a Debian box to the container.
```
journalctl -f -q | netcat 127.0.0.1 4444
```