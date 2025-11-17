# Appliquer tous les fichiers YAML
 kubectl apply -f k8s/.

# Afficher le leadearboard dans la console
kubectl logs deployment/tweetoscope-leaderboard --tail=20
