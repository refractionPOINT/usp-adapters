FROM zeek/zeek

RUN apt-get update && apt-get install -y wget
RUN wget https://downloads.limacharlie.io/adapter/linux/64 -O lc_adapter && chmod +x lc_adapter

ENV IFACE=eth0

ADD run.sh .
RUN chmod +x run.sh

# User needs to define the following environment variables:
# OID = The LimaCharlie Organization ID
# IKEY = The LimaCharlie Installation Key
# NAME = A unique name for the sensor
# =====================================
# Optional environment variables:
# IFACE = The interface to listen on (default: eth0)
# ZEEK_ARGS = Additional arguments to pass to Zeek

CMD './run.sh'
