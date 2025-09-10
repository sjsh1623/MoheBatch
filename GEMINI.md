# Project Overview

This project is a Spring Boot application written in Kotlin that uses Spring Batch for data ingestion. It appears to be a backend service that fetches data from external APIs, processes it, and stores it in a PostgreSQL database. The project is containerized using Docker and orchestrated with Docker Compose.

The project, named "MoheBatch," is a data ingestion service that reads data from external REST APIs, enriches it, and then sends it to another Spring application (`MoheSpring`) via a REST API. It uses Spring Batch for the core processing logic and is designed to be resilient and scalable, with features like chunk processing, retry logic, and a custom skip policy. The project is well-documented and includes a comprehensive test suite.

# Building and Running

The project can be built using Gradle and run using Docker Compose.

**Building the project:**

```bash
./gradlew build
```

**Running the project:**

The `docker-compose.yml` file defines several services, including the `mohebatch-web` and `mohebatch-processor` services, a PostgreSQL database, a mock API, and a monitoring stack with Prometheus and Grafana.

To run all services:

```bash
docker-compose up --build
```

To run with monitoring:

```bash
docker-compose --profile monitoring up --build
```

# Development Conventions

*   **Language:** The project is written in Kotlin.
*   **Framework:** It uses Spring Boot and Spring Batch.
*   **Build Tool:** Gradle is used for building the project.
*   **Containerization:** The project is containerized using Docker and orchestrated with Docker Compose.
*   **Database:** It uses PostgreSQL for data storage.
*   **Testing:** The project has a comprehensive test suite that includes unit, integration, and contract tests. It uses JUnit, Testcontainers, and WireMock for testing.
*   **Configuration:** Configuration is managed through environment variables and `application.yml` files.
*   **API Documentation:** The project uses SpringDoc to generate OpenAPI documentation.
