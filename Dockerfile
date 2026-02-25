FROM apache/spark:4.0.2

# 1. Switch to root for system setup
USER root

# 2. Install Python pip and basic build tools
RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 3. Set the professional working directory
WORKDIR /app

# 4. Copy and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# 5. Copy project directories into /app
COPY src/ ./src/
COPY config/ ./config/
COPY data/ ./data/

# 6. Set environment variables so Spark can still find its binaries
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 7. Ensure the 'spark' user owns the /app directory
RUN chown -R spark:spark /app

# 8. Download AWS SDK dependencies for S3 access (hadoop-aws and aws-java-sdk-bundle)
RUN curl -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 9. Switch back to the non-root 'spark' user
USER spark