# Tweetoscope - Analyse de Tendances de Hashtags en Temps Réel

## 📋 Description du Projet

Tweetoscope est une application distribuée de traitement et d'analyse de tweets en temps réel. Elle utilise Kafka pour le traitement de flux de données et fournit un tableau de bord en temps réel montrant les hashtags tendances.

## 🏗️ Architecture

Le projet est composé de plusieurs microservices :

- **KafkaTweetProducer** : Produit des tweets depuis différentes sources (API Twitter, fichiers offline, scénarios de test)
- **KafkaTweetFilter** : Filtre les tweets selon plusieurs critères (pays, langue, date de création, contenu)
- **KafkaHashtagExtractor** : Extrait les hashtags des tweets filtrés
- **KafkaHashtagCounter** : Compte les occurrences des hashtags
- **KafkaLeaderboard** : Génère un classement des hashtags tendances
- **KafkaLeaderboardWebServer** : Fournit une interface web pour visualiser les tendances

## 📦 Technologies

- **Java/Maven** : Framework principal
- **Apache Kafka** : Système de messaging pour les flux temps réel
- **Docker & Docker Compose** : Containerisation locale
- **Kubernetes** : Déploiement en cluster
- **LaTeX** : Documentation

## 🚀 Démarrage Rapide

### Avec Docker Compose

```bash
cd Tweetoscope/InitialTweetoscope
docker compose up -d
```

### Configuration des Filtres

Vous pouvez filtrer les tweets selon :
- **-f lang <code>** : Langue (ex: `en`, `fr`)
- **-f country <code>** : Pays (code ISO)
- **-f recent** : Tweets des dernières 24 heures
- **-f online** : Connexion en direct à l'API Twitter

## 📚 Documentation

Le rapport complet du projet est disponible ici : **[CR.pdf](https://tweetoscope25-group-10-lemaitre-thievon-malerba-c3b18e.pages-student.centralesupelec.fr/CR.pdf)**

## 👥 Auteurs

- James Lemaître
- François Thievon
- Louca Malerba

## 📝 Licence

Projet académique - 2025
