FROM ubuntu

RUN apt-get update && apt-get install -y wget
RUN wget https://downloads.limacharlie.io/adapter/linux/64 -O lc_adapter && chmod +x lc_adapter

ADD run.sh .
RUN chmod +x run.sh

# User needs to define the following environment variables:
# OID = The LimaCharlie Organization ID
# IKEY = The LimaCharlie Installation Key
# NAME = A unique name for the sensor
# K8S_POD_LOGS = The path to the pod logs directory (usually `/var/log/pods` within the container)

CMD './run.sh'
