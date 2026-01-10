# ============================================================
# Stage 1 — Build Flink Job (inside container)
# ============================================================
FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /build

# Cache dependencies
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

# Copy source
COPY src ./src

# Build fat jar
RUN mvn -B -q clean package -DskipTests

# ============================================================
# Stage 2 — Flink Runtime
# ============================================================
FROM flink:1.18-java17

WORKDIR /opt/flink/jobs

COPY --from=builder /build/target/flink-bronze-*.jar ./market-price-bronze.jar