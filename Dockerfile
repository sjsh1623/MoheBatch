FROM openjdk:17-jdk-slim

WORKDIR /app

# Copy Gradle wrapper and build files
COPY gradlew .
COPY gradle gradle
COPY build.gradle.kts .
COPY settings.gradle.kts .

# Copy source code
COPY src src

# Make gradlew executable and build the application
RUN chmod +x ./gradlew
RUN ./gradlew build -x test

# Run the application
EXPOSE 8080
CMD ["java", "-jar", "build/libs/MoheSpring-0.0.1-SNAPSHOT.jar"]