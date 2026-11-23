FROM apache/kafka:4.3.2

COPY --chown=appuser:appuser ./plugins/ /opt/kafka/plugins/

# Run Kafka Connect directly; no operator-generated configuration is needed.
CMD ["/opt/kafka/bin/connect-distributed.sh", "/etc/kafka-connect/connect-distributed.properties"]
