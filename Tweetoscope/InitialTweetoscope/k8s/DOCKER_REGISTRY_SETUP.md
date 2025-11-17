# Configuration du GitLab Container Registry dans Kubernetes

## Créer un secret pour l'authentification au registre privé

Avant de déployer les pods, vous devez créer un secret Kubernetes pour accéder au GitLab Container Registry :

```bash
kubectl create secret docker-registry gitlab-registry-secret \
  --docker-server=registry.gitlab.centralesupelec.fr \
  --docker-username=<your-username> \
  --docker-password=<your-personal-access-token> \
  --docker-email=<your-email> \
  -n default
```

### Ou avec un Deploy Token (recommandé)

1. Allez dans votre projet GitLab → **Settings** → **Repository** → **Deploy tokens**
2. Créez un token avec les droits `read_registry`
3. Utilisez le token :

```bash
kubectl create secret docker-registry gitlab-registry-secret \
  --docker-server=registry.gitlab.centralesupelec.fr \
  --docker-username=<deploy-token-username> \
  --docker-password=<deploy-token-password> \
  --docker-email=<your-email> \
  -n default
```

## Utiliser le secret dans les déploiements

Les fichiers YAML de déploiement doivent avoir `imagePullSecrets` :

```yaml
spec:
  template:
    spec:
      imagePullSecrets:
      - name: gitlab-registry-secret
      containers:
      - name: ...
        image: registry.gitlab.centralesupelec.fr/...
```

**Note** : Les fichiers de déploiement actuels ne contiennent pas `imagePullSecrets`. Si votre registre est privé, ajoutez cette configuration.
