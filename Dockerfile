# Build stage
FROM gradle:8.5-jdk21 AS build

WORKDIR /app

# Copy gradle files
COPY build.gradle settings.gradle ./

# Copy source code
COPY src ./src

# Build the application
RUN gradle clean build -x test --no-daemon

# Runtime stage
FROM eclipse-temurin:21-jre

# Install curl for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create directory for images
RUN mkdir -p /app/images/places

# Copy built jar from build stage
COPY --from=build /app/build/libs/*.jar app.jar

# Batch server runs on port 8081
EXPOSE 8081

# Run the application with JVM optimization settings
CMD ["java", \
    "-Djdk.httpclient.responseBufferSize=65536", \
    "-Djdk.httpclient.maxResponseHeaderSize=32768", \
    "-Dhttp.agent=MoheBatch/1.0", \
    "-Djava.net.useSystemProxies=false", \
    "-Dsun.net.http.allowRestrictedHeaders=true", \
    "-Dhttp.keepAlive=true", \
    "-Dhttp.maxConnections=20", \
    "-Xms512m", \
    "-Xmx2g", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-jar", "app.jar"]