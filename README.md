# usp-adapters
USP Adapters for various sources.

## Usage
```
docker run -it -p 4444:4444 6a9681372100 syslog debug=true syslog.port=4444 syslog.client_options.identity.ingestion_key=ab688b68-d897-4bd5-8467-6605a8a6f6b5 syslog.client_options.identity.oid=8cbe27f4-bfa1-4afb-ba19-138cd51389cd syslog.client_options.parse_hint=text "syslog.client_options.format_re=(?P<ts>[0-9.]+) +(\d+) +(?P<ip>.+) +(?P<info>.+) +(\d+) +(?P<event_type>.+) +(?P<url>.+) +(?P<user>.+) +(.+) +(.+)"
```