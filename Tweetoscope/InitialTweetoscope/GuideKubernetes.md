🚀 **Guide Rapide** : Déployer Tweetoscope sur Kubernetes
✅ Configuration Requise
    ✅ Docker Desktop installé + WSL2
    ✅ Kind installé
    ✅ kubectl installé

📋 **Étapes Simples**
1️⃣ Créer le Cluster Kubernetes (une seule fois)
> kind create cluster --config kind-config.yaml
Cela crée un cluster avec 3 nœuds (1 maître + 2 workers).

2️⃣ Déployer Tout Automatiquement
> .\deploy-k8s.ps1 -RebuildImage

    Le script fait tout :
    ✅ Build l'image Docker
    ✅ Charge dans Kind
    ✅ Déploie Kafka
    ✅ Déploie tous les microservices

3️⃣ Vérifier que Ça Fonctionne
> kubectl get pods
Tous les pods doivent être Running ✅ (appart topic creator qui est completed)

4️⃣ Accéder au Leaderboard Web
> kubectl port-forward service/tweetoscope-leaderboard 8080:8080
Puis ouvrir : http://localhost:8080


🔧 Commandes Utiles
# Voir tous les pods
kubectl get pods

# Voir les logs du producer
kubectl logs -f deployment/tweetoscope-producer

# Redémarrer un service
kubectl rollout restart deployment/tweetoscope-producer

# Supprimer tout
kubectl delete -f k8s/

# Supprimer le cluster
kind delete cluster --name kind

# Vérifier l'état de Kafka

# Voir le statut du pod
kubectl get pods -l app=kafka

# Voir les logs (pour diagnostiquer les erreurs)
kubectl logs -l app=kafka --tail=50

# Vérifier que tous les pods sont Ready
kubectl get deployments


💡 Comment Ça Marche
**Docker Compose** (local) → **Kubernetes*# kafka-deployment.yaml
volumeMounts:
  - name: kafka-data
    mountPath: /var/lib/kafka/data
volumes:
  - name: kafka-data
    nfs:  # ← Au lieu de PVC local
      server: "nfs.example.com"
      path: "/kafka"* (distribué sur 3 nœuds)
Si un nœud crash, Kubernetes redémarre les pods automatiquement ✅