FROM alpine:3.6

RUN apk add --no-cache ca-certificates

ADD _output/bin/cassandra-operator /usr/local/bin
#ADD _output/bin/cassandra-backup /usr/local/bin

CMD ["cassandra-operator"]
