FROM zeek/zeek

RUN apt-get update && apt-get install -y wget python3 python3-pip inotify-tools
RUN pip3 install limacharlie --break-system-packages
RUN wget https://downloads.limacharlie.io/adapter/linux/64 -O lc_adapter && chmod +x lc_adapter

ENV IFACE=eth0

ADD run.sh .
RUN chmod +x run.sh
ADD upload_new_files.sh .
RUN chmod +x upload_new_files.sh
ADD upload_file.sh .
RUN chmod +x upload_file.sh

# Directory where files will be extracted.
RUN mkdir extract_files

# User needs to define the following environment variables:
# OID = The LimaCharlie Organization ID
# IKEY = The LimaCharlie Installation Key
# INGESTION_TOKEN = The LimaCharlie Ingestion Token
# NAME = A unique name for the sensor
# =====================================
# Optional environment variables:
# IFACE = The interface to listen on (default: eth0)
# ZEEK_ARGS = Additional arguments to pass to Zeek

CMD './run.sh'
