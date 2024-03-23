#!/bin/bash

limacharlie artifacts upload --oid $OID --access-token $INGESTION_TOKEN --days-retention 1 --hint zeek-extract --original-path `basename "$1"` "$1"
echo "Uploaded file $1"
rm "$1"
echo "Deleted file $1"
