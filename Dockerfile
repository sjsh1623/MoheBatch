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

# Copy built jar from build stage
COPY --from=build /app/build/libs/*.jar app.jar

# Run the application with JVM HTTP 클라이언트 최적화 설정
EXPOSE 8080
CMD ["java", \
    "-Djdk.httpclient.responseBufferSize=65536", \
    "-Djdk.httpclient.maxResponseHeaderSize=32768", \
    "-Dhttp.agent=MoheSpring/1.0", \
    "-Djava.net.useSystemProxies=false", \
    "-Dsun.net.http.allowRestrictedHeaders=true", \
    "-Dhttp.keepAlive=true", \
    "-Dhttp.maxConnections=20", \
    "-Xms512m", \
    "-Xmx1g", \
    "-jar", "app.jar"]