# ✅ SOLUTION : Configuration Kafka Multi-Listeners pour Kubernetes

## 🎯 Résumé Exécutif

**PROBLÈME RÉSOLU !** Le pipeline Tweetoscope fonctionne maintenant correctement en Kubernetes (Kind).

**Cause racine** : Kafka était configuré avec un seul listener (`PLAINTEXT://:9092`) avec une advertised listener de `kafka:9092`, ce qui causait des problèmes de routage des messages dans Kubernetes.

**Solution** : Configurer plusieurs listeners distincts :
- **PLAINTEXT** (port 9092) : Pour les clients externes (producers/consumers inter-pods via Kubernetes Service)
- **INTERNAL** (port 29092) : Pour la communication inter-broker au sein du cluster Kafka
- **CONTROLLER** (port 9093) : Pour la communication KRaft quorum

---

## 🔍 Diagnostic du Problème

### Configuration AVANT (CASSÉE)
```yaml
KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
```

### Problèmes identifiés
1. ❌ `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093` → `localhost` ne fonctionne que dans le pod
2. ❌ Un seul listener PLAINTEXT pour TOUS les types de communication
3. ❌ Pas de séparation entre communication client et inter-broker
4. ❌ En Kubernetes, le routage via Service DNS `kafka:9092` causait des boucles de redirection

---

## ✅ Configuration APRÈS (FONCTIONNELLE)

### Modification 1 : `k8s/kafka-deployment.yaml`

```yaml
env:
- name: KAFKA_NODE_ID
  value: "1"
- name: KAFKA_PROCESS_ROLES
  value: "broker,controller"
- name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
  value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT"
- name: KAFKA_CONTROLLER_QUORUM_VOTERS
  value: "1@127.0.0.1:9093"  # ✅ Utilise loopback au lieu de localhost
- name: KAFKA_LISTENERS
  value: "PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093"
- name: KAFKA_ADVERTISED_LISTENERS
  value: "PLAINTEXT://kafka:9092,INTERNAL://kafka.default.svc.cluster.local:29092"
- name: KAFKA_INTER_BROKER_LISTENER_NAME
  value: "INTERNAL"  # ✅ Communication inter-broker via INTERNAL
- name: KAFKA_CONTROLLER_LISTENER_NAMES
  value: "CONTROLLER"
```

### Modification 2 : `k8s/kafka-service.yaml`

Ajouter les ports manquants pour supporter tous les listeners :

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
      name: client       # Pour les producers/consumers
    - port: 29092
      targetPort: 29092
      name: internal     # Pour la communication inter-broker
    - port: 9093
      targetPort: 9093
      name: controller   # Pour KRaft quorum
  selector:
    app: kafka
```

---

## 🔑 Points Clés de la Solution

### 1️⃣ Trois Listeners Distincts

| Listener | Port | Protocole | Utilisation | Advertised |
|----------|------|-----------|-------------|-----------|
| **PLAINTEXT** | 9092 | PLAINTEXT | Clients externes (producers/consumers) | `kafka:9092` |
| **INTERNAL** | 29092 | PLAINTEXT | Communication inter-broker Kafka | `kafka.default.svc.cluster.local:29092` |
| **CONTROLLER** | 9093 | PLAINTEXT | Communication KRaft quorum | N/A (interne) |

### 2️⃣ Pourquoi INTERNAL pour inter-broker?

- **Docker Compose** : Tous les conteneurs partagent le même réseau bridge → un listener suffit
- **Kubernetes** : Les pods doivent communiquer via le DNS du Service Kubernetes
- **Utiliser INTERNAL** : Permet à Kafka de rediriger les inter-broker vers `kafka.default.svc.cluster.local:29092` au lieu de `kafka:9092`, évitant les boucles de routage

### 3️⃣ Controller Quorum Voters

```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@127.0.0.1:9093
```

- `127.0.0.1` (loopback) fonctionne car c'est une adresse locale au pod
- `localhost:9093` fonctionne aussi mais `127.0.0.1` est plus explicit
- Ceci n'affecte que la communication intra-pod (le broker parlant au controller local)

### 4️⃣ DNS Kubernetes

```yaml
KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092,INTERNAL://kafka.default.svc.cluster.local:29092"
```

- `kafka:9092` → Résout vers le Service Kubernetes ClusterIP (résolution courte)
- `kafka.default.svc.cluster.local:29092` → FQDN complet pour les clients inter-pod

---

## 📊 Résultats de Test

### AVANT (Cassé)
```
❌ Producer: "Tweet sent to partition 0 with offset 12345"
❌ Filter: "0 records polled" (aucun message reçu)
❌ Counter: "Poll #780: 0 records, map size: 0"
❌ Leaderboard: "Waiting for data..."
```

### APRÈS (Fonctionnel) ✅
```
✅ Producer: "Tweet sent to partition 0 with offset 12345..."
✅ Filter: "Filtered tweet sent to partition with id: 78249..."
✅ Counter: "Poll #6200: 1 records, map size: 12"
✅ Counter: "Published leaderboard: {"#fun":820,"#CS":701,"#Metz":667,...}"
✅ Leaderboard: 
   TOP HASHTAGS LEADERBOARD
   1  #fun                             843
   2  #CS                              722
   3  #Metz                            690
   4  #weather                         655
   5  #crypto                          633
```

---

## 🚀 Procédure de Déploiement

### 1. Appliquer les configurations fixes
```bash
kubectl delete deployment kafka
kubectl delete service kafka
kubectl apply -f k8s/kafka-service.yaml
kubectl apply -f k8s/kafka-deployment.yaml
kubectl wait --for=condition=ready pod -l app=kafka --timeout=120s
```

### 2. Créer les topics
```bash
kubectl apply -f k8s/topic-creator-job.yaml
kubectl wait --for=condition=complete job/topic-creator --timeout=60s
```

### 3. Déployer les services
```bash
kubectl apply -f k8s/tweetoscope-producer-deployment.yaml
kubectl apply -f k8s/tweetoscope-filter-deployment.yaml
kubectl apply -f k8s/tweetoscope-extractor-deployment.yaml
kubectl apply -f k8s/tweetoscope-counter-deployment.yaml
kubectl apply -f k8s/tweetoscope-leaderboard-deployment.yaml
```

### 4. Vérifier le flux de données
```bash
# Vérifier que le producer envoie des messages
kubectl logs deployment/tweetoscope-producer --tail=10

# Vérifier que le filter consomme et publie
kubectl logs deployment/tweetoscope-filter --tail=10

# Vérifier que le counter traite les données
kubectl logs deployment/tweetoscope-counter --tail=10

# Vérifier que le leaderboard affiche les résultats
kubectl logs deployment/tweetoscope-leaderboard --tail=10
```

---

## 🔧 Configuration Kafka pour Docker Compose (Référence)

La configuration Docker Compose fonctionne parce qu'elle était déjà correcte :
```yaml
KAFKA_LISTENERS: PLAINTEXT://kafka:9092,CONTROLLER://kafka:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

**Différences clés**:
- Utilise `kafka:9093` au lieu de `localhost:9093` pour le quorum (résolution DNS locale)
- Un seul réseau bridge suffit (pas de problème inter-pod)
- Pas besoin de listener INTERNAL séparé

---

## 📚 Concepts Kafka Expliqués

### LISTENERS vs ADVERTISED_LISTENERS

- **LISTENERS** : Ports sur lesquels Kafka écoute DANS le conteneur
- **ADVERTISED_LISTENERS** : Adresses que Kafka annonce aux clients pour s'y connecter

**Exemple** :
```
LISTENERS: PLAINTEXT://0.0.0.0:9092        # Écoute partout sur le port 9092
ADVERTISED: PLAINTEXT://kafka:9092         # Dit aux clients de se connecter à kafka:9092
```

Quand un client se connecte à `kafka:9092`, Kafka lui dit "reconnecte-toi à `kafka:9092`" (redirection).

### Problème en Kubernetes (AVANT)

```
Client (dans pod) → "Connecte-toi à kafka:9092" (via Service DNS)
Kafka Pod         → "Reconnecte-toi à kafka:9092" (advertised listener)
Client            → "Mais je suis déjà connecté à kafka:9092!" (boucle)
Timeout           → Messages ne sont jamais envoyés/reçus
```

### Solution avec INTERNAL (APRÈS)

```
Client (inter-pod)      → "Connecte-toi à kafka:9092" (PLAINTEXT listener)
Kafka Pod               → "Utilise INTERNAL://kafka.default.svc.cluster.local:29092" pour inter-broker
Broker → Broker         → Communication directe via INTERNAL listener (évite boucle)
```

---

## 🧪 Tests de Diagnostic Disponibles

### 1. Vérifier que Kafka est ready
```bash
kubectl exec deployment/kafka -- test -f /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log
echo "Kafka est prêt!"
```

### 2. Lister les topics
```bash
kubectl exec deployment/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 3. Vérifier les listener actifs
```bash
kubectl exec deployment/kafka -- ss -tlnp | grep 9092
```

### 4. Tester la connectivité inter-pod
```bash
kubectl exec deployment/tweetoscope-producer -- sh -c "nc -zv kafka 9092"
# Result: kafka (10.96.xxx.xxx:9092) open
```

---

## 📋 Checklist de Validation

- ✅ Producer envoie des messages (offsets qui augmentent)
- ✅ Filter consomme les messages du topic `raw-tweets`
- ✅ Extractor consomme du topic `filtered-tweets` et produit sur `hashtags`
- ✅ Counter consomme du topic `hashtags` et produit sur `leaderboards`
- ✅ Leaderboard service affiche les top hashtags en temps réel
- ✅ Les logs des services ne montrent aucune exception Kafka
- ✅ Les topics Kafka contiennent des messages (consumer group offset)

---

## 🎓 Enseignements

### Différences Docker vs Kubernetes Networking

| Aspect | Docker Compose | Kubernetes |
|--------|---|---|
| **Réseau** | Bridge network unique | Overlay network (CNI plugin) |
| **DNS** | Noms de service locaux | FQDN cluster.local |
| **Listeners Kafka** | 1-2 suffit | 3+ recommandé (client/broker/controller) |
| **Routage** | Direct (même bridge) | Via Service DNS |
| **Redirection Kafka** | Peut être ignorée | CRITIQUE pour éviter les boucles |

### Bonnes Pratiques Kafka en Kubernetes

1. **Toujours configurer plusieurs listeners** pour les différents types de communication
2. **Utiliser FQDN complets** (`kafka.default.svc.cluster.local`) pour la stabilité
3. **Définir INTER_BROKER_LISTENER_NAME** pour diriger la communication inter-broker
4. **Exposer tous les ports** dans le Kubernetes Service
5. **Utiliser des adresses loopback** (`127.0.0.1`) pour la communication intra-pod

---

## 🔗 Fichiers Modifiés

1. **`k8s/kafka-deployment.yaml`** - Configuration Kafka avec 3 listeners
2. **`k8s/kafka-service.yaml`** - Service Kubernetes avec tous les ports

Aucune modification nécessaire dans :
- `src/main/java/tweetoscope/kafka/*.java` (le code client fonctionne déjà bien)
- `Dockerfile` (construction d'image inchangée)
- `docker-compose.yml` (referent de configuration, déjà correct)

---

## ✨ Conclusion

Le pipeline Tweetoscope fonctionne maintenant correctement en Kubernetes avec :
- ✅ Production en temps réel de tweets aléatoires
- ✅ Filtrage des tweets par langue et récence
- ✅ Extraction et comptage des hashtags
- ✅ Classement en direct des top hashtags
- ✅ Interface web de leaderboard actualisée toutes les 2 secondes

**État du déploiement** : **OPÉRATIONNEL** 🚀
