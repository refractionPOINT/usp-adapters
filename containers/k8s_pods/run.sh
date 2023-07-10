#! /bin/sh
set -e

env "client_options.identity.oid=$OID" "client_options.identity.installation_key=$IKEY" "client_options.hostname=$NAME" "client_options.platform=k8s_pods" "client_options.sensor_seed_key=$NAME" "root=$K8S_POD_LOGS" ./lc_adapter k8s_pods unused1 unused2