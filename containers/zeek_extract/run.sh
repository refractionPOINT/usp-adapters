#! /bin/sh
set -e

./upload_new_files.sh &

export ZEEK_HOME=/usr/local/zeek
zeek -i $IFACE -C LogAscii::use_json=T LogAscii::output_to_stdout=T $ZEEK_ARGS policy/frameworks/files/extract-all-files.zeek | env "client_options.identity.oid=$OID" "client_options.identity.installation_key=$IKEY" "client_options.hostname=$NAME" "client_options.platform=json" "client_options.sensor_seed_key=$NAME" ./lc_adapter stdin unused1 unused2