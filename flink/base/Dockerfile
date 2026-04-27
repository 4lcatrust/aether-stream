FROM flink:1.18-java17

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop && \
    curl -L \
      https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.0/flink-s3-fs-hadoop-1.18.0.jar \
      -o /opt/flink/plugins/flink-s3-fs-hadoop/flink-s3-fs-hadoop.jar
      
RUN curl -L \
      https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.0/flink-metrics-prometheus-1.18.0.jar \
      -o /opt/flink/lib/flink-metrics-prometheus.jar