#!/usr/bin/env pwsh
# Script de déploiement automatisé pour Tweetoscope sur Kubernetes (Kind)

param(
    [switch]$RebuildImage = $false,
    [switch]$SkipKafkaWait = $false
)

$ErrorActionPreference = "Stop"

Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "   🚀 Déploiement Tweetoscope sur Kubernetes (Kind)        " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Vérifier que Kind est installé
if (-not (Get-Command kind -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Erreur: Kind n'est pas installé!" -ForegroundColor Red
    Write-Host "   Installez Kind: https://kind.sigs.k8s.io/docs/user/quick-start/" -ForegroundColor Yellow
    exit 1
}

# Vérifier que kubectl est installé
if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Erreur: kubectl n'est pas installé!" -ForegroundColor Red
    exit 1
}

# Vérifier que Docker est en cours d'exécution
try {
    docker info | Out-Null
} catch {
    Write-Host "❌ Erreur: Docker n'est pas en cours d'exécution!" -ForegroundColor Red
    exit 1
}

# Vérifier si le cluster Kind existe
$clusterExists = kind get clusters 2>$null | Select-String -Pattern "^kind$"
if (-not $clusterExists) {
    Write-Host "⚠️  Le cluster Kind n'existe pas. Création du cluster..." -ForegroundColor Yellow
    if (Test-Path "kind-config.yaml") {
        kind create cluster --config kind-config.yaml
    } else {
        kind create cluster
    }
    Write-Host "✅ Cluster Kind créé avec succès!" -ForegroundColor Green
}

# Build de l'image Docker si demandé ou si elle n'existe pas
$imageExists = docker images fthievon/tweetoscope-kafka:latest --format "{{.Repository}}" | Select-String -Pattern "fthievon/tweetoscope-kafka"
if ($RebuildImage -or -not $imageExists) {
    Write-Host ""
    Write-Host "🔨 Construction de l'image Docker..." -ForegroundColor Cyan
    docker build -t fthievon/tweetoscope-kafka:latest .
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Erreur lors de la construction de l'image!" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✅ Image Docker construite avec succès!" -ForegroundColor Green
    
    # Vérifier le contenu de l'image
    Write-Host ""
    Write-Host "📦 Contenu de l'image (JARs disponibles):" -ForegroundColor Cyan
    docker run --rm fthievon/tweetoscope-kafka:latest ls -la /app/jars/
} else {
    Write-Host ""
    Write-Host "INFO: Utilisation de l'image Docker existante" -ForegroundColor Blue
    Write-Host "      (Utilisez -RebuildImage pour forcer la reconstruction)" -ForegroundColor Gray
}

# Charger l'image dans Kind
Write-Host ""
Write-Host "📦 Chargement de l'image dans le cluster Kind..." -ForegroundColor Cyan
kind load docker-image fthievon/tweetoscope-kafka:latest --name kind

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Erreur lors du chargement de l'image dans Kind!" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Image chargée dans Kind!" -ForegroundColor Green

# Déploiement de Kafka
Write-Host ""
Write-Host "🚀 Déploiement de l'infrastructure Kafka..." -ForegroundColor Cyan

kubectl apply -f k8s/kafka-pvc.yaml
kubectl apply -f k8s/kafka-deployment.yaml
kubectl apply -f k8s/kafka-service.yaml

if (-not $SkipKafkaWait) {
    Write-Host ""
    Write-Host "⏳ Attente du démarrage de Kafka (peut prendre 1-2 minutes)..." -ForegroundColor Yellow
    
    try {
        kubectl wait --for=condition=ready pod -l app=kafka --timeout=180s
        Write-Host "✅ Kafka est prêt!" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Timeout en attendant Kafka. Vérifiez les logs:" -ForegroundColor Yellow
        Write-Host "   kubectl logs -l app=kafka" -ForegroundColor Gray
    }
}

# Création des topics Kafka
Write-Host ""
Write-Host "📝 Création des topics Kafka..." -ForegroundColor Cyan

kubectl delete job topic-creator --ignore-not-found=true
kubectl apply -f k8s/topic-creator-job.yaml

Write-Host "⏳ Attente de la création des topics..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

try {
    kubectl wait --for=condition=complete job/topic-creator --timeout=60s
    Write-Host "✅ Topics Kafka créés!" -ForegroundColor Green
    
    # Afficher les logs du job
    Write-Host ""
    Write-Host "📋 Topics créés:" -ForegroundColor Cyan
    kubectl logs job/topic-creator | Select-String -Pattern "raw-tweets|filtered-tweets|hashtags|leaderboards"
} catch {
    Write-Host "⚠️  Le job de création des topics n'est pas terminé" -ForegroundColor Yellow
    Write-Host "   Vérifiez les logs: kubectl logs job/topic-creator" -ForegroundColor Gray
}

# Déploiement des microservices Tweetoscope
Write-Host ""
Write-Host "🚀 Déploiement des microservices Tweetoscope..." -ForegroundColor Cyan

# Ordre de déploiement selon la pipeline de données
Write-Host "   → Producer..." -ForegroundColor White
kubectl apply -f k8s/tweetoscope-producer-deployment.yaml

Write-Host "   → Filter..." -ForegroundColor White
kubectl apply -f k8s/tweetoscope-filter-deployment.yaml

Write-Host "   → Extractor..." -ForegroundColor White
kubectl apply -f k8s/tweetoscope-extractor-deployment.yaml

Write-Host "   → Counter..." -ForegroundColor White
kubectl apply -f k8s/tweetoscope-counter-deployment.yaml

Write-Host "   → Leaderboard..." -ForegroundColor White
kubectl apply -f k8s/tweetoscope-leaderboard-deployment.yaml
kubectl apply -f k8s/tweetoscope-leaderboard-service.yaml

Write-Host "✅ Tous les microservices sont déployés!" -ForegroundColor Green

# Attendre quelques secondes pour que les pods démarrent
Write-Host ""
Write-Host "⏳ Attente du démarrage des pods (10 secondes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Afficher le statut des pods
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "   📊 Statut des Pods                                       " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
kubectl get pods

# Afficher les services
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "   🌐 Services Disponibles                                  " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
kubectl get services

# Vérifier si des pods sont en erreur
Write-Host ""
$failedPods = kubectl get pods --field-selector=status.phase!=Running,status.phase!=Succeeded --no-headers 2>&1 | Where-Object {$_ -notmatch "No resources found"}
if ($failedPods) {
    Write-Host "⚠️  Certains pods ne sont pas en état Running:" -ForegroundColor Yellow
    Write-Host $failedPods
    Write-Host ""
    Write-Host "💡 Commandes de diagnostic:" -ForegroundColor Cyan
    Write-Host "   kubectl describe pod [nom-du-pod]" -ForegroundColor Gray
    Write-Host "   kubectl logs [nom-du-pod]" -ForegroundColor Gray
    Write-Host "   kubectl logs [nom-du-pod] --previous" -ForegroundColor Gray
} else {
    Write-Host "✅ Tous les pods sont en cours d'exécution!" -ForegroundColor Green
}

# Instructions finales
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "   ✅ Déploiement Terminé!                                  " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "💡 Commandes utiles:" -ForegroundColor Cyan
Write-Host "   • Voir les logs du leaderboard:" -ForegroundColor White
Write-Host "     kubectl logs -f deployment/tweetoscope-leaderboard" -ForegroundColor Gray
Write-Host ""
Write-Host "   • Accéder au leaderboard web:" -ForegroundColor White
Write-Host "     kubectl port-forward service/tweetoscope-leaderboard 8080:8080" -ForegroundColor Gray
Write-Host "     Puis ouvrir: http://localhost:8080" -ForegroundColor Gray
Write-Host ""
Write-Host "   • Voir tous les logs:" -ForegroundColor White
Write-Host "     kubectl logs -f deployment/tweetoscope-producer" -ForegroundColor Gray
Write-Host "     kubectl logs -f deployment/tweetoscope-filter" -ForegroundColor Gray
Write-Host "     kubectl logs -f deployment/tweetoscope-extractor" -ForegroundColor Gray
Write-Host "     kubectl logs -f deployment/tweetoscope-counter" -ForegroundColor Gray
Write-Host ""
Write-Host "   • Redémarrer un déploiement:" -ForegroundColor White
Write-Host "     kubectl rollout restart deployment/[nom-deployment]" -ForegroundColor Gray
Write-Host ""
Write-Host "   • Nettoyer tout:" -ForegroundColor White
Write-Host "     kubectl delete -f k8s/" -ForegroundColor Gray
Write-Host ""
