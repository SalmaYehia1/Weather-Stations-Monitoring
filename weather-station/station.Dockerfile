FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/target/weather-station-1.0-SNAPSHOT.jar app.jar
VOLUME /data/station
ENTRYPOINT ["java", "-cp", "app.jar", "com.weather.WeatherProducer"]
CMD ["1"]
