FROM python:3.10-slim

# Install git + Java 21 (compatible Spark 4)
RUN apt-get update && apt-get install -y \
    git \
    openjdk-21-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Java 21
ENV JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Install PipelineDP from GitHub
RUN pip install git+https://github.com/OpenMined/PipelineDP.git#egg=pipeline_dp

COPY dp_app/ /app/dp_app


CMD ["python", "-m", "dp_app.main"]

