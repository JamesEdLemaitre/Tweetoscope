#!/usr/bin/env pwsh
# Script de diagnostic pour Tweetoscope sur Kubernetes

$ErrorActionPreference = "Stop"

Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "   🔍 Diagnostic Tweetoscope sur Kubernetes                " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# 1. Vérifier le statut des pods
Write-Host "📊 Statut des Pods:" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Gray
kubectl get pods -o wide

# 2. Identifier les pods en erreur
Write-Host ""
Write-Host "🔴 Pods en erreur (CrashLoopBackOff, Error, etc.):" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Gray
$failedPods = kubectl get pods --field-selector=status.phase!=Running,status.phase!=Succeeded --no-headers 2>$null

if ($failedPods) {
    Write-Host $failedPods -ForegroundColor Red
    
    # Obtenir plus de détails sur chaque pod en erreur
    $podNames = $failedPods | ForEach-Object { ($_ -split '\s+')[0] }
    
    foreach ($podName in $podNames) {
        Write-Host ""
        Write-Host "─────────────────────────────────────────────────────────" -ForegroundColor Yellow
        Write-Host "📋 Détails du pod: $podName" -ForegroundColor Yellow
        Write-Host "─────────────────────────────────────────────────────────" -ForegroundColor Yellow
        
        # Description du pod
        Write-Host ""
        Write-Host "🔍 Description (events):" -ForegroundColor White
        kubectl describe pod $podName | Select-String -Pattern "Events:" -Context 0,20
        
        # Logs actuels
        Write-Host ""
        Write-Host "📄 Logs actuels:" -ForegroundColor White
        kubectl logs $podName --tail=20 2>&1
        
        # Logs précédents (si le pod a redémarré)
        Write-Host ""
        Write-Host "📄 Logs précédents (si redémarrage):" -ForegroundColor White
        kubectl logs $podName --previous --tail=20 2>&1
    }
} else {
    Write-Host "✅ Aucun pod en erreur!" -ForegroundColor Green
}

# 3. Vérifier les services
Write-Host ""
Write-Host "🌐 Services:" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Gray
kubectl get services

# 4. Vérifier les events récents
Write-Host ""
Write-Host "📅 Events récents (derniers 10):" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Gray
kubectl get events --sort-by='.lastTimestamp' | Select-Object -Last 10

# 5. Vérifier l'image dans les pods
Write-Host ""
Write-Host "🐳 Images utilisées par les pods:" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Gray
kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\n"}{end}' | Format-Table

# 6. Vérifier les variables d'environnement
Write-Host ""
Write-Host "🔧 Variables d'environnement SERVICE_JAR:" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Gray

$deployments = @("tweetoscope-producer", "tweetoscope-filter", "tweetoscope-extractor", "tweetoscope-counter", "tweetoscope-leaderboard")

foreach ($deployment in $deployments) {
    $serviceJar = kubectl get deployment $deployment -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SERVICE_JAR")].value}' 2>$null
    if ($serviceJar) {
        Write-Host "  ✅ $deployment : SERVICE_JAR=$serviceJar" -ForegroundColor Green
    } else {
        Write-Host "  ❌ $deployment : SERVICE_JAR non défini!" -ForegroundColor Red
    }
}

# 7. Vérifier le contenu de l'image Docker
Write-Host ""
Write-Host "📦 Vérification du contenu de l'image Docker:" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Gray

$imageExists = docker images fthievon/tweetoscope-kafka:latest --format "{{.Repository}}" | Select-String -Pattern "fthievon/tweetoscope-kafka"

if ($imageExists) {
    Write-Host "✅ Image locale trouvée" -ForegroundColor Green
    Write-Host ""
    Write-Host "📂 JARs disponibles dans l'image:" -ForegroundColor White
    docker run --rm fthievon/tweetoscope-kafka:latest ls -la /app/jars/
} else {
    Write-Host "❌ Image locale non trouvée!" -ForegroundColor Red
    Write-Host "   Exécutez: docker build -t fthievon/tweetoscope-kafka:latest ." -ForegroundColor Yellow
}

# 8. Vérifier les topics Kafka
Write-Host ""
Write-Host "📋 Topics Kafka:" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Gray

$kafkaPod = kubectl get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}' 2>$null

if ($kafkaPod) {
    Write-Host "✅ Pod Kafka trouvé: $kafkaPod" -ForegroundColor Green
    Write-Host ""
    Write-Host "📝 Liste des topics:" -ForegroundColor White
    kubectl exec $kafkaPod -- /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>$null
} else {
    Write-Host "❌ Pod Kafka non trouvé!" -ForegroundColor Red
}

# 9. Résumé et recommandations
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "   💡 Recommandations                                       " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

if ($failedPods) {
    Write-Host "🔧 Actions à effectuer:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "1. Si l'erreur est 'Unable to access jarfile':" -ForegroundColor White
    Write-Host "   • Vérifiez que la variable SERVICE_JAR est définie" -ForegroundColor Gray
    Write-Host "   • Vérifiez que les JARs existent dans /app/jars/" -ForegroundColor Gray
    Write-Host "   • Reconstruisez l'image: docker build -t fthievon/tweetoscope-kafka:latest ." -ForegroundColor Gray
    Write-Host "   • Rechargez dans Kind: kind load docker-image fthievon/tweetoscope-kafka:latest" -ForegroundColor Gray
    Write-Host ""
    Write-Host "2. Si Kafka ne démarre pas:" -ForegroundColor White
    Write-Host "   • Vérifiez les logs: kubectl logs -l app=kafka" -ForegroundColor Gray
    Write-Host "   • Supprimez le PVC: kubectl delete pvc kafka-pvc" -ForegroundColor Gray
    Write-Host "   • Redéployez: kubectl apply -f k8s/" -ForegroundColor Gray
    Write-Host ""
    Write-Host "3. Pour redémarrer un déploiement:" -ForegroundColor White
    Write-Host "   kubectl rollout restart deployment/[nom-deployment]" -ForegroundColor Gray
    Write-Host ""
} else {
    Write-Host "✅ Tous les pods fonctionnent correctement!" -ForegroundColor Green
    Write-Host ""
    Write-Host "🎉 Votre application est opérationnelle!" -ForegroundColor Green
    Write-Host ""
    Write-Host "   Pour accéder au leaderboard:" -ForegroundColor White
    Write-Host "   kubectl port-forward service/tweetoscope-leaderboard 8080:8080" -ForegroundColor Gray
    Write-Host "   Puis ouvrir: http://localhost:8080" -ForegroundColor Gray
    Write-Host ""
}

Write-Host "📚 Commandes utiles:" -ForegroundColor Cyan
Write-Host "   • Voir les logs en temps réel:" -ForegroundColor White
Write-Host "     kubectl logs -f deployment/[nom]" -ForegroundColor Gray
Write-Host "   • Se connecter à un pod:" -ForegroundColor White
Write-Host "     kubectl exec -it [nom-pod] -- sh" -ForegroundColor Gray
Write-Host "   • Redéployer complètement:" -ForegroundColor White
Write-Host "     .\deploy-k8s.ps1 -RebuildImage" -ForegroundColor Gray
Write-Host ""
