#! /bin/sh
set -e

zeek -i $IFACE -C LogAscii::use_json=T LogAscii::output_to_stdout=T | env "client_options.identity.oid=$OID" "client_options.identity.installation_key=$IKEY" "client_options.hostname=$NAME" "client_options.platform=json" "client_options.sensor_seed_key=$NAME" ./lc_adapter stdin unused1 unused2