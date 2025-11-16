# 📝 Résumé des Changements - Fix Kafka Kubernetes

## Fichiers Modifiés

### 1. `k8s/kafka-deployment.yaml`

**Sections modifiées** : Configuration des variables d'environnement Kafka

#### AVANT (Cassé)
```yaml
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
```

#### APRÈS (Fonctionnel)
```yaml
env:
- name: KAFKA_NODE_ID
  value: "1"
- name: KAFKA_PROCESS_ROLES
  value: "broker,controller"
- name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
  value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT"
- name: KAFKA_CONTROLLER_QUORUM_VOTERS
  value: "1@127.0.0.1:9093"
- name: KAFKA_LISTENERS
  value: "PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093"
- name: KAFKA_ADVERTISED_LISTENERS
  value: "PLAINTEXT://kafka:9092,INTERNAL://kafka.default.svc.cluster.local:29092"
- name: KAFKA_INTER_BROKER_LISTENER_NAME
  value: "INTERNAL"
- name: KAFKA_CONTROLLER_LISTENER_NAMES
  value: "CONTROLLER"
- name: KAFKA_AUTO_CREATE_TOPICS_ENABLE
  value: "true"
- name: KAFKA_CLUSTER_ID
  value: "MkwNYQ3MQ1OUscuL1SrJqQ"
- name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
  value: "1"
- name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
  value: "1"
- name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
  value: "1"
```

**Changements clés** :
- ✅ Ajout du listener INTERNAL sur le port 29092
- ✅ Modification de LISTENER_SECURITY_PROTOCOL_MAP pour inclure INTERNAL
- ✅ CONTROLLER_QUORUM_VOTERS: `localhost` → `127.0.0.1` (meilleure pratique)
- ✅ LISTENERS: Spécification explicite des adresses et ports
- ✅ ADVERTISED_LISTENERS: Ajout du listener INTERNAL avec FQDN complet
- ✅ Ajout de INTER_BROKER_LISTENER_NAME pour diriger communication inter-broker
- ✅ CLUSTER_ID → KAFKA_CLUSTER_ID (cohérence de nommage)
- ✅ Ajout des variables de replication factor manquantes

---

### 2. `k8s/kafka-service.yaml`

**Sections modifiées** : Ports du Kubernetes Service

#### AVANT (Incomplet)
```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  type: ClusterIP
  ports:
    - port: 9092
      targetPort: 9092
      name: client
  selector:
    app: kafka
```

#### APRÈS (Complet)
```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  type: ClusterIP
  ports:
    - port: 9092
      targetPort: 9092
      name: client
    - port: 29092
      targetPort: 29092
      name: internal
    - port: 9093
      targetPort: 9093
      name: controller
  selector:
    app: kafka
```

**Changements clés** :
- ✅ Ajout du port 29092 (INTERNAL listener)
- ✅ Ajout du port 9093 (CONTROLLER listener)
- ✅ Noms explicites pour chaque port

---

## Commandes de Déploiement Exécutées

### 1. Suppression de l'ancienne configuration
```bash
kubectl delete deployment kafka
kubectl delete service kafka
Start-Sleep -Seconds 5
```

### 2. Déploiement de la nouvelle configuration
```bash
kubectl apply -f k8s/kafka-service.yaml
kubectl apply -f k8s/kafka-deployment.yaml
```

### 3. Attendre que Kafka soit prêt
```bash
kubectl wait --for=condition=ready pod -l app=kafka --timeout=120s
```

### 4. Créer les topics
```bash
kubectl apply -f k8s/topic-creator-job.yaml
```

### 5. Déployer les services
```bash
kubectl apply -f k8s/tweetoscope-producer-deployment.yaml
kubectl apply -f k8s/tweetoscope-filter-deployment.yaml
kubectl apply -f k8s/tweetoscope-extractor-deployment.yaml
kubectl apply -f k8s/tweetoscope-counter-deployment.yaml
kubectl apply -f k8s/tweetoscope-leaderboard-deployment.yaml
```

---

## Résultats Observés

### Avant la Fix
```
Producer:    "Tweet sent to partition 0 with offset X" ✅
Filter:      "0 records polled" ❌
Counter:     "Poll #780: 0 records, map size: 0" ❌
Leaderboard: "Waiting for data..." ❌
```

### Après la Fix
```
Producer:    "Tweet sent to partition 0 with offset X" ✅
             → Offsets augmentent continuellement (27098+)

Filter:      "Filtered tweet sent to partition with id: Y" ✅
             → Traite des centaines de messages

Counter:     "Poll #11420: 2 records, map size: 12" ✅
             → Leaderboard: {"#fun":1581, "#Metz":1275, ...}

Leaderboard: Top hashtags affichés ✅
             → Updates toutes les 2 secondes
```

---

## Vérification Post-Déploiement

```bash
# 1. Vérifier que tous les pods sont running
kubectl get pods
# Résultat : Tous les pods en 1/1 Running

# 2. Vérifier le flux producer → filter
kubectl logs deployment/tweetoscope-producer --tail=3
kubectl logs deployment/tweetoscope-filter --tail=3
# Résultat : Les offsets correspondent

# 3. Vérifier la sortie du counter
kubectl logs deployment/tweetoscope-counter --tail=5
# Résultat : Leaderboard publié avec des hashtags et comptages

# 4. Vérifier l'interface leaderboard
kubectl logs deployment/tweetoscope-leaderboard --tail=10
# Résultat : TOP HASHTAGS LEADERBOARD avec le classement
```

---

## Points Importants

### ❌ ERREURS À ÉVITER

1. **Ne pas supprimer le pod Kafka avant d'appliquer les nouveaux manifests**
   - Kubernetes peut recréer l'ancien avec l'ancienne configuration
   - Solution : `kubectl delete deployment` + `kubectl delete service` d'abord

2. **Ne pas oublier d'exposer les nouveaux ports dans le Service**
   - Sans ça, les inter-broker listeners ne sont pas accessibles

3. **Ne pas utiliser `localhost` dans CONTROLLER_QUORUM_VOTERS**
   - Utiliser `127.0.0.1` ou le nom du pod complet

4. **Ne pas oublier INTER_BROKER_LISTENER_NAME**
   - Sinon Kafka utilisera PLAINTEXT pour tout, causant des boucles de routage

### ✅ BONNES PRATIQUES APPLIQUÉES

1. Expliciter les adresses d'écoute (`0.0.0.0:port` au lieu de `:port`)
2. Utiliser des adresses IP pour la communication locale (`127.0.0.1`)
3. Utiliser des FQDN pour la communication inter-pod (`kafka.default.svc.cluster.local`)
4. Nommer les listeners de manière explicite (PLAINTEXT, INTERNAL, CONTROLLER)
5. Documenter le rôle de chaque listener

---

## Fichiers Non Modifiés (mais testés)

- ✅ `src/main/java/tweetoscope/kafka/KafkaTweetProducer.java` → Fonctionne correctement
- ✅ `src/main/java/tweetoscope/kafka/KafkaTweetFilter.java` → Fonctionne correctement
- ✅ `src/main/java/tweetoscope/kafka/KafkaHashtagExtractor.java` → Fonctionne correctement
- ✅ `src/main/java/tweetoscope/kafka/KafkaHashtagCounter.java` → Fonctionne correctement
- ✅ `src/main/java/tweetoscope/kafka/KafkaLeaderboardWebServer.java` → Fonctionne correctement
- ✅ `Dockerfile` → Pas de modification nécessaire
- ✅ `docker-compose.yml` → Configuration de référence, déjà correcte

---

## Temps de Déploiement

- **Suppression ancienne config** : ~5 secondes
- **Déploiement Kafka** : ~30 secondes
- **Création topics** : ~5 secondes
- **Déploiement services** : Immédiat (image déjà pullée)
- **Total** : ~40 secondes

---

## Validation Finale

```bash
# Status du cluster
kubectl get all

# Output attendu :
# NAME                                       READY   STATUS      RESTARTS   AGE
# pod/kafka-XXXXX                            1/1     Running     0          2m
# pod/tweetoscope-counter-XXXXX              1/1     Running     0          2m
# pod/tweetoscope-extractor-XXXXX            1/1     Running     0          2m
# pod/tweetoscope-filter-XXXXX               1/1     Running     0          2m
# pod/tweetoscope-leaderboard-XXXXX          1/1     Running     0          2m
# pod/tweetoscope-producer-XXXXX             1/1     Running     0          2m
# pod/topic-creator-XXXXX                    0/1     Completed   0          3m
# ...
# service/kafka                              ClusterIP  10.96.xxx.xxx  <none>  9092/TCP,29092/TCP,9093/TCP
```

✅ **STATUS** : **OPÉRATIONNEL** 🚀
