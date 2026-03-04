# Multi-stage build for Metalog (Node)
# Stage 1: Build
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# Copy all pom.xml files first (dependency caching layer)
COPY pom.xml .
COPY core/pom.xml core/
COPY grpc-server/pom.xml grpc-server/
COPY kafka-ingestion/pom.xml kafka-ingestion/
COPY benchmarks/pom.xml benchmarks/
RUN mvn dependency:go-offline

# Copy source code and build
COPY core/src core/src
COPY grpc-server/src grpc-server/src
COPY kafka-ingestion/src kafka-ingestion/src
COPY benchmarks/src benchmarks/src
RUN mvn clean package -DskipTests -pl grpc-server -am

# Stage 2: Runtime
FROM eclipse-temurin:17-jre
WORKDIR /app

# Install CLP binary from .deb (resolves shared lib deps automatically)
COPY clp-core_*.deb /tmp/
RUN apt-get update \
    && dpkg -i /tmp/clp-core_*.deb || apt-get -f install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* /tmp/clp-core_*.deb

# Create staging/output dirs for CLP compression
RUN mkdir -p /tmp/clp-staging /tmp/clp-output

# Copy fat JAR from build stage
COPY --from=build /build/grpc-server/target/metalog-grpc-server-*.jar /app/metalog.jar

# Default config path (mount or override via CLI arg)
ENTRYPOINT ["java", "-jar", "/app/metalog.jar"]
CMD ["/etc/clp/node.yaml"]
