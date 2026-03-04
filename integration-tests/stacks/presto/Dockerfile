FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY core/pom.xml core/
COPY grpc-server/pom.xml grpc-server/
COPY kafka-ingestion/pom.xml kafka-ingestion/
COPY benchmarks/pom.xml benchmarks/
RUN mvn dependency:go-offline
COPY core/src core/src
COPY grpc-server/src grpc-server/src
COPY kafka-ingestion/src kafka-ingestion/src
COPY benchmarks/src benchmarks/src
RUN mvn clean package -DskipTests -pl grpc-server -am

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/grpc-server/target/metalog-grpc-server-*.jar /app/metalog.jar
COPY --from=build /build/grpc-server/target/lib/ /app/lib/
