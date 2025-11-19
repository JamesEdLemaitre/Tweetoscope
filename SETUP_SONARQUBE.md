# Configuration SonarQube pour Tweetoscope

## 📋 Résumé

Vous avez un GitLab auto-hébergé par l'école, donc SonarCloud (qui nécessite OAuth GitLab) ne peut pas être utilisé.

**Solution : SonarQube Community Edition** (version self-hosted, gratuite, open-source)

Le pipeline CI/CD a été configuré pour lancer automatiquement :
1. Un conteneur **SonarQube** 
2. Une analyse Maven avec le SonarScanner
3. Les résultats sont générés automatiquement

---

## ✅ Configuration Déjà Faite

Le fichier `.gitlab-ci.yml` contient maintenant un job `sonarqube-scan` qui :

1. **Lance un conteneur SonarQube** dans le job
2. **Attend que SonarQube soit prêt** (health check)
3. **Lance `mvn verify sonar:sonar`** pour analyser le code
4. **Envoie les résultats** à SonarQube
5. **Archive les rapports** comme artifacts

---

## 🚀 Utilisation

### À chaque push sur `main` ou `master` :

1. Le job `sonarqube-scan` démarre
2. Un conteneur SonarQube se lance en arrière-plan
3. Maven analyse le code et envoie les résultats
4. Les résultats sont visibles dans les **logs du job**
5. Les rapports sont sauvegardés comme artifacts

### Pour voir les rapports :

1. Aller dans **CI/CD** → **Pipelines** sur GitLab
2. Cliquer sur le job `sonarqube-scan`
3. Aller dans **Artifacts** → Télécharger les rapports
4. Ou lire directement les logs pour les problèmes détectés

---

## 📊 Métriques Analysées

SonarQube va vérifier :

| Métrique | Description |
|---|---|
| **Bugs** | Défauts logiques qui causeront des erreurs |
| **Code Smells** | Mauvaises pratiques et code peu maintenable |
| **Security Issues** | Failles de sécurité |
| **Hotspots** | Code suspect nécessitant une review |
| **Duplications** | Code dupliqué inutile |

---

## 🔧 Variables d'Environnement

```yaml
SONAR_HOST_URL: http://sonarqube:9000      # URL du serveur SonarQube
SONAR_LOGIN: admin                         # Identifiant par défaut
SONAR_PASSWORD: admin                      # Mot de passe par défaut
```

**Note :** L'authentification utilise les credentials par défaut de SonarQube Community.

---

## 📈 Améliorations Futures (Optionnel)

Si vous voulez améliorer la setup :

### 1. **Ajouter JaCoCo pour la couverture de tests**
```xml
<!-- Dans pom.xml -->
<plugin>
    <groupId>org.jacoco</groupId>
    <artifactId>jacoco-maven-plugin</artifactId>
    <version>0.8.10</version>
</plugin>
```

### 2. **Persister les données SonarQube**
Créer une instance SonarQube permanente avec Docker Compose pour conserver l'historique.

### 3. **Ajouter un Quality Gate**
Configurer SonarQube pour rejeter les MR si la qualité baisse.

---

## ✅ Vérifier que ça Marche

1. Faire un `git push` sur `main` ou `master`
2. Aller dans **CI/CD** → **Pipelines**
3. Attendre que le job `sonarqube-scan` finisse (2-3 min)
4. Cliquer sur le job → **Logs**
5. Vous verrez le rapport SonarQube à la fin des logs avec :
   - ✅ Nombre de bugs
   - ✅ Code smells
   - ✅ Security issues
   - ✅ Hotspots
   - ✅ Duplications

---

## 🐛 Troubleshooting

### Le job timeout (prend trop de temps)
→ SonarQube peut être lent à démarrer. Augmenter le timeout du job dans GitLab si nécessaire.

### "Connection refused" à SonarQube
→ SonarQube prend du temps à démarrer. Le script attend 30 secondes, c'est généralement suffisant.

### Les artifacts sont vides
→ Normal, SonarQube génère les rapports mais les logs contiennent les infos principales.

---

**Le pipeline est prêt ! 🚀 Faites un push pour voir l'analyse en action.**

