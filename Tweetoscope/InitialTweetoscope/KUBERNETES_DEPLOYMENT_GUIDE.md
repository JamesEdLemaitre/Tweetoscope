# 🚀 Guide de Déploiement Kubernetes

## ✅ Problèmes Corrigés

### 1. **Erreur "Unable to access jarfile"**
- **Cause** : Les deployments utilisaient une commande hardcodée avec un mauvais nom de JAR
- **Solution** : Utilisation de la variable d'environnement `SERVICE_JAR` comme dans Docker Compose

### 2. **Configuration des Deployments**
Tous les fichiers dans `k8s/` ont été mis à jour :
- `tweetoscope-producer-deployment.yaml` → `SERVICE_JAR: "KafkaTweetProducer"`
- `tweetoscope-filter-deployment.yaml` → `SERVICE_JAR: "KafkaTweetFilter"`
- `tweetoscope-extractor-deployment.yaml` → `SERVICE_JAR: "KafkaHashtagExtractor"`
- `tweetoscope-counter-deployment.yaml` → `SERVICE_JAR: "KafkaHashtagCounter"`
- `tweetoscope-leaderboard-deployment.yaml` → `SERVICE_JAR: "KafkaLeaderboard"`

### 3. **Variables d'Environnement Ajoutées**
Chaque microservice a maintenant les bonnes variables :
- `KAFKA_BOOTSTRAP_SERVERS`
- `TOPIC_INPUT` (pour les consumers)
- `TOPIC_OUTPUT` (pour les producers)

---

## 📋 Prérequis

- **Docker Desktop** avec WSL2 activé
- **Kind** installé ([Installation](https://kind.sigs.k8s.io/docs/user/quick-start/))
- **kubectl** installé ([Installation](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/))

---

## 🎯 Déploiement Rapide

### Option 1 : Déploiement Automatisé (Recommandé)

```powershell
# Déploiement complet avec rebuild de l'image
.\deploy-k8s.ps1 -RebuildImage

# Ou déploiement avec l'image existante
.\deploy-k8s.ps1
```

### Option 2 : Déploiement Manuel

```powershell
# 1. Build de l'image Docker
docker build -t fthievon/tweetoscope-kafka:latest .

# 2. Vérifier le contenu de l'image
docker run --rm fthievon/tweetoscope-kafka:latest ls -la /app/jars/

# 3. Créer le cluster Kind (si nécessaire)
kind create cluster --config kind-config.yaml

# 4. Charger l'image dans Kind
kind load docker-image fthievon/tweetoscope-kafka:latest --name kind

# 5. Déployer Kafka
kubectl apply -f k8s/kafka-pvc.yaml
kubectl apply -f k8s/kafka-deployment.yaml
kubectl apply -f k8s/kafka-service.yaml

# 6. Attendre que Kafka soit prêt
kubectl wait --for=condition=ready pod -l app=kafka --timeout=180s

# 7. Créer les topics
kubectl apply -f k8s/topic-creator-job.yaml
kubectl wait --for=condition=complete job/topic-creator --timeout=60s

# 8. Déployer les microservices
kubectl apply -f k8s/tweetoscope-producer-deployment.yaml
kubectl apply -f k8s/tweetoscope-filter-deployment.yaml
kubectl apply -f k8s/tweetoscope-extractor-deployment.yaml
kubectl apply -f k8s/tweetoscope-counter-deployment.yaml
kubectl apply -f k8s/tweetoscope-leaderboard-deployment.yaml
kubectl apply -f k8s/tweetoscope-leaderboard-service.yaml

# 9. Vérifier le statut
kubectl get pods
```

---

## 🔍 Diagnostic et Dépannage

### Script de Diagnostic Automatique

```powershell
.\diagnose-k8s.ps1
```

Ce script vérifie :
- ✅ Statut de tous les pods
- ✅ Pods en erreur avec logs détaillés
- ✅ Variables d'environnement SERVICE_JAR
- ✅ Contenu de l'image Docker
- ✅ Topics Kafka créés
- ✅ Recommandations personnalisées

### Commandes de Diagnostic Manuelles

```powershell
# Voir tous les pods
kubectl get pods

# Logs d'un pod spécifique
kubectl logs -f deployment/tweetoscope-leaderboard

# Logs d'un pod qui crash
kubectl logs <nom-pod> --previous

# Description détaillée d'un pod
kubectl describe pod <nom-pod>

# Events récents
kubectl get events --sort-by='.lastTimestamp'

# Se connecter à un pod
kubectl exec -it <nom-pod> -- sh

# Lister les fichiers dans le conteneur
kubectl exec <nom-pod> -- ls -la /app/jars/
```

---

## 🌐 Accéder à l'Application

### Leaderboard Web

```powershell
# Port-forward vers le service leaderboard
kubectl port-forward service/tweetoscope-leaderboard 8080:8080
```

Puis ouvrir dans le navigateur : **http://localhost:8080**

### Vérifier les Topics Kafka

```powershell
# Obtenir le nom du pod Kafka
$kafkaPod = kubectl get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}'

# Lister les topics
kubectl exec $kafkaPod -- /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Consommer des messages d'un topic
kubectl exec -it $kafkaPod -- /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic leaderboards `
  --from-beginning
```

---

## 🔄 Mise à Jour de l'Application

### Après une Modification du Code

```powershell
# 1. Rebuild l'image
docker build -t fthievon/tweetoscope-kafka:latest .

# 2. Recharger dans Kind
kind load docker-image fthievon/tweetoscope-kafka:latest --name kind

# 3. Redémarrer les deployments
kubectl rollout restart deployment/tweetoscope-producer
kubectl rollout restart deployment/tweetoscope-filter
kubectl rollout restart deployment/tweetoscope-extractor
kubectl rollout restart deployment/tweetoscope-counter
kubectl rollout restart deployment/tweetoscope-leaderboard

# Ou redéployer complètement
.\deploy-k8s.ps1 -RebuildImage
```

---

## 🧹 Nettoyage

### Supprimer Tous les Déploiements

```powershell
# Supprimer tous les ressources
kubectl delete -f k8s/

# Supprimer le cluster Kind
kind delete cluster --name kind
```

### Redémarrer un Seul Microservice

```powershell
# Supprimer et recréer un deployment
kubectl delete -f k8s/tweetoscope-leaderboard-deployment.yaml
kubectl apply -f k8s/tweetoscope-leaderboard-deployment.yaml

# Ou simplement redémarrer
kubectl rollout restart deployment/tweetoscope-leaderboard
```

---

## ❓ Problèmes Courants

### 1. **Pods en CrashLoopBackOff**

**Diagnostic :**
```powershell
kubectl logs <nom-pod> --previous
kubectl describe pod <nom-pod>
```

**Solutions :**
- Vérifier que `SERVICE_JAR` est défini
- Vérifier que l'image contient les JARs : `docker run --rm fthievon/tweetoscope-kafka:latest ls /app/jars/`
- Rebuild et reload l'image dans Kind

### 2. **ImagePullBackOff**

**Cause :** L'image n'est pas chargée dans Kind

**Solution :**
```powershell
kind load docker-image fthievon/tweetoscope-kafka:latest --name kind
kubectl rollout restart deployment/<nom-deployment>
```

### 3. **Kafka ne Démarre Pas**

**Solutions :**
```powershell
# Voir les logs
kubectl logs -l app=kafka

# Supprimer le PVC et redémarrer
kubectl delete pvc kafka-pvc
kubectl delete pod -l app=kafka
```

### 4. **Topics Kafka Non Créés**

**Vérification :**
```powershell
kubectl logs job/topic-creator
```

**Solution :**
```powershell
kubectl delete job topic-creator
kubectl apply -f k8s/topic-creator-job.yaml
```

---

## 📊 Architecture de Déploiement

```
┌─────────────────────────────────────────────────────────────┐
│                    Cluster Kubernetes (Kind)                 │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐      ┌──────────────┐                     │
│  │   Kafka      │◄─────┤ Topic        │                     │
│  │  (Broker)    │      │  Creator     │                     │
│  └──────┬───────┘      └──────────────┘                     │
│         │                                                     │
│         │ Topics: raw-tweets, filtered-tweets,               │
│         │         hashtags, leaderboards                     │
│         │                                                     │
│  ┌──────▼───────┐      ┌──────────────┐                     │
│  │   Producer   │─────►│    Filter    │                     │
│  └──────────────┘      └──────┬───────┘                     │
│                                │                              │
│                         ┌──────▼───────┐                     │
│                         │  Extractor   │                     │
│                         └──────┬───────┘                     │
│                                │                              │
│                         ┌──────▼───────┐                     │
│                         │   Counter    │                     │
│                         └──────┬───────┘                     │
│                                │                              │
│                         ┌──────▼───────┐                     │
│                         │ Leaderboard  │◄────── Port 8080    │
│                         └──────────────┘                     │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 📚 Ressources

- [Documentation Kind](https://kind.sigs.k8s.io/)
- [Documentation Kubernetes](https://kubernetes.io/docs/home/)
- [Documentation Apache Kafka](https://kafka.apache.org/documentation/)

---

## ✅ Checklist de Déploiement

- [ ] Docker Desktop est démarré
- [ ] Kind est installé
- [ ] kubectl est installé
- [ ] Cluster Kind créé
- [ ] Image Docker buildée
- [ ] Image chargée dans Kind
- [ ] Kafka déployé et ready
- [ ] Topics créés
- [ ] Microservices déployés
- [ ] Tous les pods en Running
- [ ] Leaderboard accessible sur http://localhost:8080

---

**🎉 Bonne chance avec votre déploiement Tweetoscope sur Kubernetes !**
