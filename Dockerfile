FROM flink:1.12.3-scala_2.12-java11
COPY --chown=flink:flink target/flink-async-http-example-1.0-SNAPSHOT.jar /opt/flink/usrlib/
