# 🔴 Problème : Producer Kafka envoie des messages mais aucun consumer ne les reçoit

## 📋 Contexte du Projet

### Architecture
- **Application** : Tweetoscope - Pipeline de traitement de tweets en temps réel
- **Technologies** : Java 11, Apache Kafka, Kubernetes (Kind)
- **Environnement** : Windows 10/11 avec Docker Desktop + WSL2
- **Cluster** : Kind (Kubernetes in Docker) avec 1 control-plane + 2 workers

### Pipeline de Traitement
```
Producer → Kafka (raw-tweets) → Filter → Kafka (filtered-tweets) → Extractor → Kafka (hashtags) → Counter → Kafka (leaderboards) → Leaderboard Web UI
```

### Configuration Actuelle
- **Image Docker** : `fthievon/tweetoscope-kafka:latest` (même image pour tous les microservices)
- **Kafka** : Apache Kafka (image `apache/kafka:latest`) en mode KRaft (sans Zookeeper)
- **Broker Kafka** : Accessible via service Kubernetes `kafka:9092`
- **Topics créés** : `raw-tweets`, `filtered-tweets`, `hashtags`, `leaderboards` (tous avec 1 partition, replication factor 1)

---

## 🚨 Symptômes du Problème

### 1. **Producer semble fonctionner**
```
Tweet sent to partition 0 with offset 247584
Tweet sent to partition 0 with offset 247585
Tweet sent to partition 0 with offset 247586
```
✅ Pas d'exception
✅ Offsets qui augmentent constamment
✅ Callbacks de succès reçus

### 2. **Consumer ne reçoit AUCUN message**
```bash
$ kubectl exec deployment/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw-tweets --from-beginning --timeout-ms 5000

# Résultat : Processed a total of 0 messages
```

### 3. **Counter affiche 0 records en permanence**
```
[Counter] Poll #780: 0 records, map size: 0
[Counter] Poll #790: 0 records, map size: 0
```

### 4. **Leaderboard Web UI affiche**
```
Waiting for data...
Updates every 2 seconds
```

### 5. **Connectivité réseau OK**
```bash
$ kubectl exec deployment/tweetoscope-producer -- sh -c "nc -zv kafka 9092"
# Résultat : kafka (10.96.218.139:9092) open
```
✅ Le producer peut joindre Kafka sur le réseau

---

## 🔍 Informations Techniques Détaillées

### Configuration du Producer (KafkaTweetProducer.java)
```java
private static final String TOPIC_NAME = "raw-tweets"; // Hardcodé

private Properties configureKafkaProducer() {
    Properties producerProperties = new Properties();
    String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
        bootstrapServers = "kafka:9092"; // Default
    }
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.VoidSerializer");
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
    return producerProperties;
}

// Envoi des messages
ProducerRecord<Void, String> record = new ProducerRecord<>(TOPIC_NAME, null, tweetJson);
kafkaProducer.send(record, (metadata, exception) -> {
    if (exception != null) {
        System.err.println("Error sending tweet: " + exception.getMessage());
    } else {
        System.out.println("Tweet sent to partition " + metadata.partition() + " with offset " + metadata.offset());
    }
});
```

### Deployment Kubernetes du Producer
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tweetoscope-producer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tweetoscope-producer
  template:
    metadata:
      labels:
        app: tweetoscope-producer
    spec:
      containers:
      - name: producer
        image: fthievon/tweetoscope-kafka:latest
        env:
        - name: SERVICE_JAR
          value: "KafkaTweetProducer"
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        - name: TOPIC_OUTPUT
          value: "raw-tweets"
        - name: ARGS
          value: "random"
```

### Dockerfile (ENTRYPOINT)
```dockerfile
FROM eclipse-temurin:11-jre-alpine
WORKDIR /app
RUN mkdir -p /app/jars
COPY --from=builder /build/target/*-jar-with-dependencies.jar /app/jars/

ENV SERVICE_JAR=""
ENV KAFKA_BOOTSTRAP_SERVERS="kafka:9092"

ENTRYPOINT ["sh", "-c", "java -Djava.awt.headless=true -jar /app/jars/${SERVICE_JAR}-jar-with-dependencies.jar ${ARGS:-}"]
```

### Configuration Kafka (Deployment Kubernetes)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: apache/kafka:latest
        env:
        - name: KAFKA_NODE_ID
          value: "1"
        - name: KAFKA_PROCESS_ROLES
          value: "broker,controller"
        - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
          value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
        - name: KAFKA_CONTROLLER_QUORUM_VOTERS
          value: "1@localhost:9093"
        - name: KAFKA_LISTENERS
          value: "PLAINTEXT://:9092,CONTROLLER://:9093"
        - name: KAFKA_ADVERTISED_LISTENERS
          value: "PLAINTEXT://kafka:9092"
        - name: KAFKA_CONTROLLER_LISTENER_NAMES
          value: "CONTROLLER"
        - name: KAFKA_AUTO_CREATE_TOPICS_ENABLE
          value: "true"
        - name: CLUSTER_ID
          value: "MkwNYQ3MQ1OUscuL1SrJqQ"
        ports:
        - containerPort: 9092
        - containerPort: 9093
```

### Service Kafka
```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  selector:
    app: kafka
  ports:
    - name: kafka-client
      protocol: TCP
      port: 9092
      targetPort: 9092
    - name: kafka-controller
      protocol: TCP
      port: 9093
      targetPort: 9093
```

---

## 🤔 Hypothèses sur la Cause du Problème

### Hypothèse 1 : Configuration réseau Kubernetes vs Docker Compose
- **Docker Compose** : Tous les conteneurs partagent le même réseau bridge, communication directe
- **Kubernetes** : Communication via Services (DNS + ClusterIP), peut nécessiter configuration spécifique Kafka

**Problème potentiel** : `KAFKA_ADVERTISED_LISTENERS` est configuré pour `kafka:9092` mais les clients Kubernetes doivent peut-être utiliser une adresse différente.

### Hypothèse 2 : Différence localhost vs nom du service
- Dans le pod Kafka, les outils utilisent `localhost:9092`
- Les producers externes utilisent `kafka:9092`
- Possible problème de binding/listener dans Kafka

### Hypothèse 3 : Messages buffurisés non flushés
- Le producer Kafka en Java peut bufferiser les messages
- Si `flush()` n'est pas appelé ou si le buffer n'est pas plein, les messages restent en mémoire
- Mais le callback de succès suggère que les messages sont bien envoyés

### Hypothèse 4 : Metadata/Partition routing
- Kafka retourne des metadata avec partition/offset corrects
- Mais les messages ne sont peut-être pas écrits dans le bon topic ou la bonne partition
- Possible problème de configuration des listeners multiples

### Hypothèse 5 : Problème de chemin/fichiers
- Le mode `random` ne nécessite pas de fichiers (génère des tweets aléatoires)
- Mais peut-être un problème dans la génération des tweets eux-mêmes

---

## ✅ Ce qui Fonctionne

1. ✅ Cluster Kubernetes Kind démarré et fonctionnel
2. ✅ Kafka pod en Running (1/1 READY)
3. ✅ Tous les microservices en Running (Producer, Filter, Extractor, Counter, Leaderboard)
4. ✅ Topics Kafka créés avec succès (confirmé par `kafka-topics.sh --list`)
5. ✅ Connectivité réseau entre Producer et Kafka (nc test réussi)
6. ✅ Producer démarre sans erreur et affiche les callbacks de succès
7. ✅ Offsets augmentent continuellement (preuve que Kafka accepte les messages ?)
8. ✅ Image Docker buildée correctement avec tous les JARs

---

## ❌ Ce qui Ne Fonctionne Pas

1. ❌ Impossible de consommer des messages depuis `raw-tweets` avec `kafka-console-consumer.sh`
2. ❌ Le Filter ne reçoit aucun message (pas de logs de traitement)
3. ❌ Le Counter affiche 0 records en permanence
4. ❌ Le Leaderboard reste bloqué sur "Waiting for data..."
5. ❌ Aucun message visible dans aucun topic Kafka

---

## 🎯 Question pour l'Agent

**Comment diagnostiquer et résoudre ce problème où le producer Kafka semble envoyer des messages avec succès (callbacks OK, offsets qui augmentent) mais aucun consumer ne peut les lire, dans un environnement Kubernetes (Kind) ?**

### Sous-questions spécifiques :
1. La configuration `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092` est-elle correcte pour Kubernetes ?
2. Faut-il configurer plusieurs listeners (un pour intra-pod, un pour inter-pod) ?
3. Le producer Java écrit-il vraiment dans Kafka ou simule-t-il les offsets ?
4. Comment vérifier que les messages sont physiquement écrits dans les log files Kafka ?
5. Y a-t-il une différence de configuration nécessaire entre Docker Compose et Kubernetes pour Kafka ?

### Debugging déjà effectué :
- ✅ Vérification de la connectivité réseau (nc)
- ✅ Vérification des topics créés
- ✅ Vérification des logs du producer (pas d'erreurs)
- ✅ Tentative de consommer avec kafka-console-consumer (0 messages)
- ✅ Vérification du topic descriptor (partition/réplication OK)
- ✅ Vérification que le code utilise le bon topic name

### Fichiers disponibles pour analyse :
- `Dockerfile` (multi-stage build avec Maven)
- `docker-compose.yml` (configuration Docker Compose qui fonctionne)
- `k8s/*.yaml` (tous les manifests Kubernetes)
- `src/main/java/tweetoscope/kafka/KafkaTweetProducer.java`
- Logs complets de tous les pods disponibles

---

## 📊 État Actuel du Cluster

```bash
NAME                                       READY   STATUS      RESTARTS   AGE
kafka-b7667577f-l6t5h                      1/1     Running     0          45m
topic-creator-crl7t                        0/1     Completed   0          68s
tweetoscope-counter-549984ff9-tt56b        1/1     Running     0          46s
tweetoscope-extractor-7679fcdf7-zp9t7      1/1     Running     0          46s
tweetoscope-filter-7df45d6cd9-6bgh2        1/1     Running     0          46s
tweetoscope-leaderboard-7678695dd4-hf2pq   1/1     Running     0          46s
tweetoscope-producer-c7c7cf566-h8jdb       1/1     Running     0          46s
```

**Tous les pods sont Running, mais le flux de données est bloqué au niveau de Kafka.**

---

## 🆘 Besoin d'Aide Pour

1. Identifier LA cause racine du problème de communication Kafka
2. Corriger la configuration Kafka pour Kubernetes
3. Vérifier que les messages sont physiquement écrits dans Kafka
4. Faire fonctionner le flux complet de bout en bout

**Environnement** : Windows 10/11, Docker Desktop avec WSL2, Kind v0.20+, kubectl v1.28+, Apache Kafka latest
