# Architecture Docker - Tweetoscope Kafka

## Vue d'ensemble

Le système Tweetoscope repose sur une architecture microservices complète basée sur Apache Kafka et déployée en Docker. Ce document explique l'architecture complète, son fonctionnement et comment la maintenir et la déployer.

## Architecture système

```
┌─────────────────────────────────────────────────────────────────┐
│                    Docker Compose Network                       │
│              (initialtweetoscope_tweetoscope)                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────┐                                       │
│  │   Kafka Broker       │                                       │
│  │  (KRaft mode, port   │                                       │
│  │   9092)              │                                       │
│  └──────────────────────┘                                       │
│           ▲                                                     │
│           │                                                     │
│    ┌──────┴──────────────┬─────────────┬─────────────┐          │
│    │                     │             │             │          │
│    ▼                     ▼             ▼             ▼          │
│  ┌──────┐  raw-tweets   ┌──────┐  filtered-tweets ┌──────┐      │
│  │      │◄──────────────┤      │◄─────────────────┤      │      │
│  │      │               │      │                  │      │      │
│  │      │               │      │   hashtags       │      │      │
│  │      │ PRODUCER      │FILTER├─────────────────►│      │      │
│  │      │               │      │                  │EXTRACTOR    │
│  │      │               │      │              leaderboards│     │
│  │      │               │      │◄────────────────┬┘      │      │
│  └──────┘               └──────┘                 │       └──────┘
│  Port 8080              Consumes                 │
│  MockTwitter            100k+ tweets             │
│                                                  │
│  ┌──────────────────────────────────────────┐   │
│  │        COUNTER                           │  │
│  │  Aggregates hashtags from 'hashtags'     │ │
│  │  topic and publishes to 'leaderboards'   │ │
│  │  (every 1-2s)                            │ │
│  └──────────────────────────────────────────┘ │
│                                               │
│  ┌──────────────────────────────────────────┐ │
│  │        LEADERBOARD (Web UI)              │ │
│  │  Consumes 'leaderboards' topic           │ │
│  │  - HTTP Server (port 8080)               │─┘
│  │  - Web UI (HTML/CSS/JS)                  │
│  │  - JSON API (/api/leaderboard)           │
│  │  - Console output (formatted table)      │
│  └──────────────────────────────────────────┘
│
└─────────────────────────────────────────────────────────────────┘
```

## Flux de données

### 1. **Producer** → `raw-tweets` (100k+ messages)
- Génère des tweets mock aléatoires
- Chaque tweet contient : id, text, created_at, geo_location.country_code, lang
- Produit ~55k messages par session
- **Important**: `created_at` doit être défini (vérifier `MockTwitterStreamRandom.java`)

### 2. **Filter** consomme `raw-tweets` → produit `filtered-tweets` (35k+ messages)
- Filtre les tweets en fonction de critères :
  - `LangTweetFilter`: Français uniquement (`lang == "fr"`)
  - `CountryCodeTweetFilter`: France uniquement (`country_code == "FR"`)
  - `RecentTweetFilter`: Tweets récents (créés dans les 24 dernières heures)
  - `EmptyTweetFilter`: Exclut les tweets vides
- Réduit significativement le volume (35-40% des tweets passent)
- **Note critique**: Si `created_at` est null, tous les tweets sont rejetés par `RecentTweetFilter`

### 3. **Extractor** consomme `filtered-tweets` → produit `hashtags` (15k+ messages)
- Extrait les hashtags de chaque tweet
- Format : `{tweet_id, hashtag, frequency}`
- Produit un message par hashtag trouvé

### 4. **Counter** consomme `hashtags` → produit `leaderboards` (1k+ messages)
- Agrège les hashtags avec une fenêtre glissante
- Compte les occurrences de chaque hashtag
- Publie le classement complet toutes les 1-2 secondes
- Format JSON : `{"#fun":5347,"#CS":4438,"#Metz":4368,...}`

### 5. **Leaderboard** consomme `leaderboards`
- Affiche les résultats en temps réel via trois canaux :
  - **Web UI** (HTTP) : http://localhost:8080 (auto-refresh chaque 2s)
  - **JSON API** : http://localhost:8080/api/leaderboard
  - **Console** : Tableau formaté imprimé en permanence

---

## Structure des fichiers Docker

```
InitialTweetoscope/
├── docker-compose.yml          # Orchestration des 6 services
├── Dockerfile                  # Image multi-stage (Maven + JRE)
├── README_DOCKER.md           # Ce fichier
├── pom.xml                     # Dépendances Maven
└── src/main/java/tweetoscope/
    ├── kafka/
    │   ├── KafkaTweetProducer.java         # Service 1: Génère tweets
    │   ├── KafkaTweetFilter.java           # Service 2: Filtre tweets
    │   ├── KafkaHashtagExtractor.java      # Service 3: Extrait hashtags
    │   ├── KafkaHashtagCounter.java        # Service 4: Agrège hashtags
    │   ├── KafkaLeaderboard.java           # Service 5: Affiche résultats
    │   └── KafkaLeaderboardWebServer.java  # Serveur HTTP pour UI
    ├── tweetsFilter/
    │   ├── TweetFilter.java
    │   ├── LangTweetFilter.java
    │   ├── CountryCodeTweetFilter.java
    │   ├── RecentTweetFilter.java
    │   └── EmptyTweetFilter.java
    └── tweetsProducer/
        ├── TweetsProducer.java
        ├── MockTwitterStreamRandom.java    # ⚠️ CRITICAL: doit set created_at
        ├── MockTwitterStreamRecorded.java
        └── MockTwitterStreamScenario.java
```

---

## Services Docker

### 1. **kafka**
- **Image**: `apache/kafka:latest`
- **Port**: 9092 (Bootstrap server)
- **Mode**: KRaft (sans Zookeeper)
- **Sujets créés** :
  - `raw-tweets` (3 partitions)
  - `filtered-tweets` (3 partitions)
  - `hashtags` (3 partitions)
  - `leaderboards` (1 partition)

### 2. **tweetoscope-producer**
- **Image**: `fthievon/tweetoscope-kafka:latest`
- **Commande**: `java -cp /app/jars/* tweetoscope.kafka.KafkaTweetProducer`
- **Dépend de**: kafka (healthcheck)
- **Logs**: `docker logs tweetoscope-producer`

### 3. **tweetoscope-filter**
- **Image**: `fthievon/tweetoscope-kafka:latest`
- **Commande**: `java -cp /app/jars/* tweetoscope.kafka.KafkaTweetFilter`
- **Consumer Group**: `KafkaTweetFilter-Group`
- **Dépend de**: kafka, tweetoscope-producer

### 4. **tweetoscope-extractor**
- **Image**: `fthievon/tweetoscope-kafka:latest`
- **Commande**: `java -cp /app/jars/* tweetoscope.kafka.KafkaHashtagExtractor`
- **Consumer Group**: `myOwnPrivateJavaGroup`
- **Dépend de**: kafka, tweetoscope-filter

### 5. **tweetoscope-counter**
- **Image**: `fthievon/tweetoscope-kafka:latest`
- **Commande**: `java -cp /app/jars/* tweetoscope.kafka.KafkaHashtagCounter`
- **Consumer Group**: `myOwnPrivateJavaGroup`
- **Dépend de**: kafka, tweetoscope-extractor

### 6. **tweetoscope-leaderboard**
- **Image**: `fthievon/tweetoscope-kafka:latest`
- **Commande**: `java -cp /app/jars/* tweetoscope.kafka.KafkaLeaderboard true` (mode headless)
- **Port**: 8080 (mapé à l'hôte)
- **Dépend de**: kafka, tweetoscope-counter
- **Endpoints** :
  - `GET /` : Page HTML interactive
  - `GET /api/leaderboard` : JSON avec classement actuel

---

## Configuration Kafka

### Variables d'environnement
Tous les services utilisent `KAFKA_BOOTSTRAP_SERVERS` pour se connecter à Kafka :
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

Cette variable est lue par tous les services. Si elle n'est pas définie, fallback à `"kafka:9092"` (DNS du container).

### Consumer Groups
- `KafkaTweetFilter-Group`: Filter (1 consumer actif)
- `myOwnPrivateJavaGroup`: Extractor et Counter (2 consumers actifs)
- `visualizor-leaderboard-{timestamp}`: Leaderboard (nouveau à chaque redémarrage)

### Offset Strategy
- **Producer**: Commence au début (`earliest`)
- **Extractor & Counter**: Stratégie `latest` (ignore les anciens messages corrompus)

---

## Déploiement initial

### Prérequis
- Docker et Docker Compose installés
- Compte DockerHub (pour push l'image)
- Port 8080 libre (pour la web UI)

### 1. Construire l'image Docker
```bash
cd InitialTweetoscope
docker build -t fthievon/tweetoscope-kafka:latest .
```

**Temps estimé**: 40-50 secondes (Maven compile 26 fichiers Java)

**Output attendu** :
```
[+] Building 45.3s (16/16) FINISHED
=> [stage-1 5/5] RUN ls -la /app/jars/   0.3s
=> naming to docker.io/fthievon/tweetoscope-kafka:latest
```

### 2. Pousser l'image sur DockerHub
```bash
docker push fthievon/tweetoscope-kafka:latest
```

### 3. Démarrer les services
```bash
docker-compose up -d
```

**Temps estimé**: 1-2 minutes (services démarrent en cascade)

**Vérifier le statut** :
```bash
docker-compose ps
```

**Output attendu** :
```
NAME                      IMAGE                               STATUS
kafka                     apache/kafka:latest                 Up (healthy)
tweetoscope-producer      fthievon/tweetoscope-kafka:latest   Up (healthy)
tweetoscope-filter        fthievon/tweetoscope-kafka:latest   Up (healthy)
tweetoscope-extractor     fthievon/tweetoscope-kafka:latest   Up (healthy)
tweetoscope-counter       fthievon/tweetoscope-kafka:latest   Up (healthy)
tweetoscope-leaderboard   fthievon/tweetoscope-kafka:latest   Up (health: starting)
```

### 4. Vérifier la web UI
```bash
# Test API JSON
curl http://localhost:8080/api/leaderboard

# Ouvrir UI dans le navigateur
open http://localhost:8080  # macOS
start http://localhost:8080 # Windows
xdg-open http://localhost:8080 # Linux
```

**Output attendu (API)** :
```json
{"leaderboard":{"#fun":5347,"#CS":4438,"#Metz":4368,"#weather":4353}}
```

---

## Workflow de développement

### Scénario 1: Modifier un service Kafka (e.g., KafkaTweetFilter.java)

#### Étape 1: Faire les modifications
```bash
# Éditer le fichier
vi src/main/java/tweetoscope/kafka/KafkaTweetFilter.java
```

#### Étape 2: Rebuild l'image
```bash
docker build -t fthievon/tweetoscope-kafka:latest .
```

**Vérifier la compilation** :
- La sortie Maven doit terminer avec `BUILD SUCCESS`
- Vérifier qu'il n'y a pas d'erreurs de compilation

#### Étape 3: Push vers DockerHub (optionnel mais recommandé)
```bash
docker push fthievon/tweetoscope-kafka:latest
```

#### Étape 4: Redémarrer le service affecté
```bash
# Redémarrer SEULEMENT le service affecté
docker-compose up -d tweetoscope-filter

# OU : redémarrer complètement
docker-compose down
docker-compose up -d
```

#### Étape 5: Vérifier les logs
```bash
# Voir les logs du service modifié
docker logs tweetoscope-filter

# Logs en temps réel
docker logs -f tweetoscope-filter
```

---

### Scénario 2: Modifier plusieurs services

```bash
# 1. Modifications
vi src/main/java/tweetoscope/kafka/KafkaHashtagCounter.java
vi src/main/java/tweetoscope/kafka/KafkaLeaderboard.java

# 2. Rebuild
docker build -t fthievon/tweetoscope-kafka:latest .

# 3. Push
docker push fthievon/tweetoscope-kafka:latest

# 4. Redémarrer tous les services
docker-compose down
Start-Sleep -Seconds 2  # Ou: sleep 2 (Linux/macOS)
docker-compose up -d

# 5. Attendre le démarrage
Start-Sleep -Seconds 5
docker-compose ps
```

---

### Scénario 3: Reset complet (nettoyer tous les volumes et données)

```bash
# Arrêter tout
docker-compose down

# Supprimer les volumes (données Kafka)
docker volume prune -f

# OU : supprimer les volumes spécifiquement
docker volume rm initialtweetoscope_kafka-data

# Redémarrer propre
docker-compose up -d
```

---

## Dépannage

### Problème: Services ne démarrent pas

**Symptôme**: `docker-compose ps` affiche les services en `Exit` ou `health: starting` après 2+ minutes

**Solution**:
```bash
# 1. Vérifier les logs
docker logs tweetoscope-leaderboard

# 2. Vérifier la connectivité Kafka
docker exec tweetoscope-producer java -cp /app/jars/* tweetoscope.kafka.KafkaTweetProducer

# 3. Redémarrer
docker-compose down
docker-compose up -d
```

---

### Problème: Aucune donnée dans les topics

**Symptôme**: Topics existent mais aucun message

**Vérifier le producer**:
```bash
docker logs tweetoscope-producer | grep -i "producing\|sent\|message"
```

**Vérifier les topics**:
```bash
docker exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
docker exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic raw-tweets --from-beginning --max-messages 5
```

---

### Problème: Leaderboard ne s'actualise pas

**Vérifier le counter**:
```bash
docker logs tweetoscope-counter | tail -20
```

**Vérifier le leaderboard**:
```bash
docker logs tweetoscope-leaderboard | tail -20
curl http://localhost:8080/api/leaderboard
```

**Si JSON API vide**:
- Counter ne produit probablement pas de données
- Vérifier : `docker logs tweetoscope-counter | grep "Published leaderboard"`

---

### Problème: Port 8080 déjà utilisé

**Solution**:
```bash
# Trouver le processus
netstat -tulpn | grep 8080  # Linux
netstat -ano | findstr 8080 # Windows

# Libérer le port
kill -9 <PID>  # Linux
taskkill /F /PID <PID>  # Windows

# OU : changer le port dans docker-compose.yml
# "8080:8080" → "8081:8080"
```

---

## Monitoring

### Voir tous les logs
```bash
# Tous les services
docker-compose logs -f

# Service spécifique
docker-compose logs -f tweetoscope-counter

# Dernières N lignes
docker-compose logs --tail 50 tweetoscope-leaderboard
```

### Vérifier la santé
```bash
# Détail des services
docker-compose ps

# Inspection détaillée
docker inspect tweetoscope-leaderboard | grep -A 5 '"Health"'
```

### Monitorer les topics Kafka
```bash
# Voir les sujets
docker exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list

# Décrire un sujet
docker exec kafka kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe --topic raw-tweets

# Consommer les 10 derniers messages
docker exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic leaderboards --max-messages 10 --from-beginning
```

---

## Optimisations et configurations avancées

### 1. Changer le nombre de partitions
Éditer `docker-compose.yml` :
```yaml
environment:
  - TOPIC_PARTITIONS=5  # Au lieu de 3
```

Puis :
```bash
docker-compose down -v
docker-compose up -d
```

### 2. Augmenter la fréquence de publication du counter
Éditer `KafkaHashtagCounter.java` ligne ~90 :
```java
// Poll chaque 500ms au lieu de 1000ms
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
```

Puis rebuild :
```bash
docker build -t fthievon/tweetoscope-kafka:latest .
docker-compose up -d tweetoscope-counter
```

### 3. Changer le temps d'actualisation de la UI web
Éditer `KafkaLeaderboardWebServer.java` ligne ~195 :
```java
// Actualiser chaque 1s au lieu de 2s
setInterval(fetchLeaderboard, 1000);
```

---

## Fichiers de configuration clés

### docker-compose.yml
- Définit les 6 services et leurs dépendances
- Mappe les ports (8080 pour leaderboard)
- Définit les healthchecks
- Crée le réseau `tweetoscope`

### Dockerfile
- Build multi-stage (Maven + JRE Alpine)
- Compile avec Maven, exporte JAR
- Image runtime minimale (~300MB)

### pom.xml
- Kafka clients 4.0.0
- Twitter API SDK 2.0.3
- Gson pour JSON
- JFreeChart pour les graphiques (optionnel)

---

## Architecture réseau

```
┌────────────────────────────┐
│   Host (localhost:8080)    │
│   ↓                        │
├────────────────────────────┤
│ Docker Network             │
│ initialtweetoscope_        │
│    tweetoscope             │
│                            │
│  Services se connectent    │
│  entre eux via DNS:        │
│  - kafka:9092             │
│  - tweetoscope-producer   │
│  - tweetoscope-filter     │
└────────────────────────────┘
```

Tous les services sont sur le même réseau. La communication se fait par DNS interne à Docker (ex: `kafka:9092`).

---

## Performance et scalabilité

### Volumes actuels
- **raw-tweets**: 100k+ messages (55k/session)
- **filtered-tweets**: 35k+ messages
- **hashtags**: 15k+ messages
- **leaderboards**: 1k+ messages

### Throughput
- **Producer**: ~1000 tweets/s
- **Filter**: ~350 tweets/s (réduit par filtrages)
- **Counter**: ~2-5 top-10 leaderboards/s

### Scaling
Pour augmenter le throughput :
1. Augmenter les partitions (3 → 6)
2. Augmenter les replicas Kafka
3. Augmenter les threads de chaque service
4. Utiliser des batch sizes plus larges

---

## Checklist de déploiement

- [ ] Docker installé (`docker --version`)
- [ ] Docker Compose installé (`docker-compose --version`)
- [ ] Compte DockerHub créé et connecté (`docker login`)
- [ ] Port 8080 libre (`netstat`)
- [ ] Image buildée (`docker build`)
- [ ] Image pushée (`docker push`)
- [ ] Services démarrés (`docker-compose up -d`)
- [ ] Services healthy (`docker-compose ps`)
- [ ] API répond (`curl http://localhost:8080/api/leaderboard`)
- [ ] Web UI accessible (`http://localhost:8080`)
- [ ] Données actualisées en temps réel (rafraîchir page chaque 2s)

---

## Ressources

- [Docker Compose Docs](https://docs.docker.com/compose/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Consumer Groups](https://kafka.apache.org/documentation/#consumerconfigs)
- [Java HttpServer API](https://docs.oracle.com/javase/11/docs/api/com.sun.net.httpserver/module-summary.html)

---

## Support

Pour les problèmes :
1. Vérifier les logs : `docker-compose logs -f`
2. Vérifier la connectivité Kafka : `docker exec kafka kafka-topics.sh`
3. Tester l'API : `curl http://localhost:8080/api/leaderboard`
4. Vérifier les variables d'environnement : `docker-compose config`
5. Redémarrer complètement : `docker-compose down && docker-compose up -d`
