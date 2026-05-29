# syntax=docker/dockerfile:1
# ============================================================
# Stage 1 — Build Flink Job
# ============================================================
FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q clean package -DskipTests

# ============================================================
# Stage 2 — Runtime (MUST match JM / TM)
# ============================================================
FROM aether-flink:1.18-s3

WORKDIR /opt/flink/jobs

COPY --from=builder /build/target/bronze-jobs.jar ./bronze-jobs.jar