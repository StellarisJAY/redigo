FROM alpine:latest
MAINTAINER xxJAY

ENV address "0.0.0.0:6381"
ENV appendonly "off"
ENV appendfile "appendonly.aof"
ENV dbfilename "dump.rdb"

WORKDIR "/app"
COPY "./redigo.yaml" "/app/redigo.yaml"
COPY "./target/redigo" "/app/redigo-linux"

ENTRYPOINT ["/app/redigo-linux", "--config=redigo.yaml", "--address=$address", "--appendFilename=$appendfile", "--dbFileName=$dbfilename"]