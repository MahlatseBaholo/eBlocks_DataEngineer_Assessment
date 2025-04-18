FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y ca-certificates curl gnupg && \
    update-ca-certificates

USER 1001

# Default command (can be overridden)
CMD ["/opt/bitnami/spark/bin/spark-submit", "/app/predict_next_order_date.py"]
