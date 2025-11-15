# Multi-stage build: Maven compile stage
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build

# Copie des fichiers source et pom.xml
COPY pom.xml .
COPY src/ ./src/

# Compilation et création des JARs avec dépendances
RUN mvn clean package -DskipTests

# Stage final: Runtime avec JRE
FROM eclipse-temurin:11-jre-alpine

WORKDIR /app

# Création du répertoire pour les JARs
RUN mkdir -p /app/jars

# Copie de tous les JARs générés depuis le stage builder
COPY --from=builder /build/target/*-jar-with-dependencies.jar /app/jars/

# Vérification des JARs
RUN ls -la /app/jars/

# Variable d'environnement pour sélectionner le service à lancer
ENV SERVICE_JAR=""
ENV KAFKA_BOOTSTRAP_SERVERS="kafka:9092"

# Point d'entrée : lance le JAR correspondant au SERVICE_JAR
ENTRYPOINT ["sh", "-c", "java -Djava.awt.headless=true -jar /app/jars/${SERVICE_JAR}-jar-with-dependencies.jar"]
