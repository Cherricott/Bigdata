FROM python:3.12-slim-bookworm

# Install Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["tail", "-f", "/dev/null"]