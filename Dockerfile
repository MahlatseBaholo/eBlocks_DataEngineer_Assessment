FROM bitnami/spark:3.5.0

USER root

# Install Python and required dependencies (numpy, pandas, pymongo, etc.)
RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org -r /tmp/requirements.txt

# Ensure that Python and Spark versions are logged for debugging
RUN python --version && \
    spark-submit --version

# Create directory for extra jars
RUN mkdir -p /extra-jars

# Copy MySQL connector jar into the container
COPY ./jars/mysql-connector-j-8.0.33.jar /extra-jars/

# Set up the working directory for the application code
WORKDIR /app

# Copy application code into the container
COPY . /app

# Ensure proper permissions on the application code directory
RUN chmod -R 755 /app

# Switch back to a non-root user for security reasons
USER 1001

# Set environment variables to include the JDBC JAR
ENV SPARK_SUBMIT_OPTIONS="--jars /extra-jars/mysql-connector-j-8.0.33.jar"
ENV SPARK_JARS="/extra-jars/mysql-connector-j-8.0.33.jar"

# Default command to run the PySpark job (prediction script)
CMD ["/opt/bitnami/spark/bin/spark-submit", "--jars", "/extra-jars/mysql-connector-j-8.0.33.jar", "/app/predict_next_order_date.py"]
