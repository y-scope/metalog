# Multi-stage build for CLP Service (Node)
# Stage 1: Build
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# Copy pom.xml and download dependencies (cached layer)
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy source code and build
COPY src ./src
RUN mvn clean package -DskipTests

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
COPY --from=build /build/target/clp-service-*.jar /app/clp-service.jar

# Default config path (mount or override via CLI arg)
ENTRYPOINT ["java", "-jar", "/app/clp-service.jar"]
CMD ["/etc/clp/node.yaml"]
