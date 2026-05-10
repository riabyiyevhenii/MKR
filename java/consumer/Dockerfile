# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /build
COPY java/consumer/pom.xml .
RUN mvn dependency:go-offline -q
COPY java/consumer/src ./src
RUN mvn package -q -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /build/target/app.jar .
COPY --from=build /build/target/dependency ./dependency
ENTRYPOINT ["java", "-cp", "app.jar:dependency/*", "Main"]
