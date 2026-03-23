FROM confluentinc/cp-kafka-connect:7.6.1
USER root
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-postgres && \
    curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.6.1.Final/debezium-connector-postgres-2.6.1.Final-plugin.tar.gz \
    | tar -xz -C /usr/share/confluent-hub-components/debezium-connector-postgres --strip-components=1
USER appuser