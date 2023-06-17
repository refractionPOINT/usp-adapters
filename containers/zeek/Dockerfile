FROM zeek/zeek

RUN apt-get update && apt-get install -y wget
RUN wget https://downloads.limacharlie.io/adapter/linux/64 -O lc_adapter && chmod +x lc_adapter

ENV IFACE=eth0

ADD run.sh .
RUN chmod +x run.sh

# User needs to define the following environment variables:
# OID
# IKEY
# NAME

CMD './run.sh'
