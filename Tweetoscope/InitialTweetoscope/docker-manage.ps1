# Script d'aide pour gérer l'architecture Tweetoscope Kafka avec Docker (PowerShell pour Windows)

param(
    [Parameter(Position = 0)]
    [ValidateSet('build', 'push', 'up', 'down', 'down-v', 'logs', 'ps', 'restart', 'shell', 'kafka-topics', 'kafka-describe', 'kafka-consume', 'kafka-produce', 'clean', 'rebuild', 'help')]
    [string]$Command = 'help',
    
    [Parameter(Position = 1)]
    [string]$Argument
)

$IMAGE_NAME = "tweetoscope-kafka"
$IMAGE_TAG = "latest"

# Fonctions d'affichage
function Write-Header {
    param([string]$Message)
    Write-Host "=== $Message ===" -ForegroundColor Blue
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

# Affiche l'aide
function Show-Help {
    $helpText = @"
Usage: powershell .\docker-manage.ps1 <command> [options]

Commands:
    build [registry_user]      Build Docker image
                               Example: .\docker-manage.ps1 build thierry
    
    push [registry_user]       Push image to Docker Hub (requires 'build' first)
                               Example: .\docker-manage.ps1 push thierry
    
    up                         Start all services with docker-compose
    
    down                        Stop all services
    
    down-v                      Stop all services and remove volumes
    
    logs [service]             Show logs from all services or specific service
                               Services: producer, filter, extractor, counter, leaderboard, kafka
                               Example: .\docker-manage.ps1 logs producer
    
    ps                         Show status of all services
    
    restart [service]          Restart all services or specific service
                               Example: .\docker-manage.ps1 restart producer
    
    shell <service>            Open shell in specific service container
                               Example: .\docker-manage.ps1 shell kafka
    
    kafka-topics              List all Kafka topics
    
    kafka-describe <topic>     Describe a specific Kafka topic
                               Example: .\docker-manage.ps1 kafka-describe raw-tweets
    
    kafka-consume <topic>      Consume messages from a topic (from beginning)
                               Example: .\docker-manage.ps1 kafka-consume raw-tweets
    
    kafka-produce <topic>      Produce messages to a topic (interactive)
                               Example: .\docker-manage.ps1 kafka-produce raw-tweets
    
    clean                      Remove all containers, images and volumes
    
    rebuild [registry_user]    Clean, build and restart
                               Example: .\docker-manage.ps1 rebuild thierry
    
    help                       Show this help message

"@
    Write-Host $helpText
}

# Build Docker image
function Build-Image {
    param([string]$RegistryUser)
    
    if ([string]::IsNullOrEmpty($RegistryUser)) {
        Write-Error "Registry user not specified"
        Write-Host "Usage: .\docker-manage.ps1 build <registry_user>"
        Write-Host "Example: .\docker-manage.ps1 build thierry"
        return $false
    }
    
    Write-Header "Building Docker image: $RegistryUser/$IMAGE_NAME`:$IMAGE_TAG"
    
    if (docker build -t "$RegistryUser/$IMAGE_NAME`:$IMAGE_TAG" .) {
        Write-Success "Image built successfully: $RegistryUser/$IMAGE_NAME`:$IMAGE_TAG"
        return $true
    } else {
        Write-Error "Build failed"
        return $false
    }
}

# Push to Docker Hub
function Push-Image {
    param([string]$RegistryUser)
    
    if ([string]::IsNullOrEmpty($RegistryUser)) {
        Write-Error "Registry user not specified"
        Write-Host "Usage: .\docker-manage.ps1 push <registry_user>"
        return $false
    }
    
    Write-Header "Pushing image to Docker Hub: $RegistryUser/$IMAGE_NAME`:$IMAGE_TAG"
    
    if (docker push "$RegistryUser/$IMAGE_NAME`:$IMAGE_TAG") {
        Write-Success "Image pushed successfully"
        return $true
    } else {
        Write-Error "Push failed"
        return $false
    }
}

# Start services
function Start-Services {
    Write-Header "Starting all services with docker-compose"
    
    if (docker-compose up -d) {
        Write-Success "All services started"
        Write-Header "Service Status"
        docker-compose ps
    } else {
        Write-Error "Failed to start services"
        return $false
    }
}

# Stop services
function Stop-Services {
    Write-Header "Stopping all services"
    
    if (docker-compose down) {
        Write-Success "All services stopped"
    } else {
        Write-Error "Failed to stop services"
        return $false
    }
}

# Stop services and remove volumes
function Stop-Services-Volumes {
    Write-Header "Stopping all services and removing volumes"
    Write-Warning "This will delete all data!"
    
    $response = Read-Host "Are you sure? (y/N)"
    if ($response -eq 'y' -or $response -eq 'Y') {
        if (docker-compose down -v) {
            Write-Success "All services stopped and volumes removed"
        } else {
            Write-Error "Failed to stop services"
            return $false
        }
    } else {
        Write-Warning "Operation cancelled"
    }
}

# Show logs
function Show-Logs {
    param([string]$Service)
    
    $serviceMap = @{
        'producer'    = 'tweetoscope-producer'
        'filter'      = 'tweetoscope-filter'
        'extractor'   = 'tweetoscope-extractor'
        'counter'     = 'tweetoscope-counter'
        'leaderboard' = 'tweetoscope-leaderboard'
        'kafka'       = 'kafka'
    }
    
    if ([string]::IsNullOrEmpty($Service)) {
        Write-Header "Showing logs from all services"
        docker-compose logs -f
    } else {
        if ($serviceMap.ContainsKey($Service)) {
            $serviceName = $serviceMap[$Service]
        } else {
            Write-Error "Unknown service: $Service"
            Write-Host "Valid services: producer, filter, extractor, counter, leaderboard, kafka"
            return $false
        }
        
        Write-Header "Showing logs from service: $serviceName"
        docker-compose logs -f $serviceName
    }
}

# Show status
function Show-Status {
    Write-Header "Service Status"
    docker-compose ps
}

# Restart services
function Restart-Services {
    param([string]$Service)
    
    $serviceMap = @{
        'producer'    = 'tweetoscope-producer'
        'filter'      = 'tweetoscope-filter'
        'extractor'   = 'tweetoscope-extractor'
        'counter'     = 'tweetoscope-counter'
        'leaderboard' = 'tweetoscope-leaderboard'
        'kafka'       = 'kafka'
    }
    
    if ([string]::IsNullOrEmpty($Service)) {
        Write-Header "Restarting all services"
        docker-compose restart
    } else {
        if ($serviceMap.ContainsKey($Service)) {
            $serviceName = $serviceMap[$Service]
        } else {
            Write-Error "Unknown service: $Service"
            Write-Host "Valid services: producer, filter, extractor, counter, leaderboard, kafka"
            return $false
        }
        
        Write-Header "Restarting service: $serviceName"
        docker-compose restart $serviceName
    }
    
    Write-Success "Restart completed"
}

# Open shell in container
function Open-Shell {
    param([string]$Service)
    
    if ([string]::IsNullOrEmpty($Service)) {
        Write-Error "Service not specified"
        Write-Host "Usage: .\docker-manage.ps1 shell <service>"
        return $false
    }
    
    $serviceMap = @{
        'producer'    = 'tweetoscope-producer'
        'filter'      = 'tweetoscope-filter'
        'extractor'   = 'tweetoscope-extractor'
        'counter'     = 'tweetoscope-counter'
        'leaderboard' = 'tweetoscope-leaderboard'
        'kafka'       = 'kafka'
    }
    
    if ($serviceMap.ContainsKey($Service)) {
        $serviceName = $serviceMap[$Service]
    } else {
        Write-Error "Unknown service: $Service"
        Write-Host "Valid services: producer, filter, extractor, counter, leaderboard, kafka"
        return $false
    }
    
    Write-Header "Opening shell in service: $serviceName"
    docker-compose exec $serviceName bash 2>$null || docker-compose exec $serviceName sh
}

# List Kafka topics
function List-Topics {
    Write-Header "Kafka Topics"
    docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
}

# Describe Kafka topic
function Describe-Topic {
    param([string]$Topic)
    
    if ([string]::IsNullOrEmpty($Topic)) {
        Write-Error "Topic not specified"
        Write-Host "Usage: .\docker-manage.ps1 kafka-describe <topic>"
        return $false
    }
    
    Write-Header "Topic Description: $Topic"
    docker exec kafka kafka-topics.sh --describe --topic $Topic --bootstrap-server localhost:9092
}

# Consume from Kafka topic
function Consume-Topic {
    param([string]$Topic)
    
    if ([string]::IsNullOrEmpty($Topic)) {
        Write-Error "Topic not specified"
        Write-Host "Usage: .\docker-manage.ps1 kafka-consume <topic>"
        return $false
    }
    
    Write-Header "Consuming from topic: $Topic (Press Ctrl+C to stop)"
    docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $Topic --from-beginning
}

# Produce to Kafka topic
function Produce-Topic {
    param([string]$Topic)
    
    if ([string]::IsNullOrEmpty($Topic)) {
        Write-Error "Topic not specified"
        Write-Host "Usage: .\docker-manage.ps1 kafka-produce <topic>"
        return $false
    }
    
    Write-Header "Producing to topic: $Topic (Press Ctrl+C to stop)"
    docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $Topic
}

# Clean everything
function Clean-All {
    Write-Header "Cleaning all containers, images and volumes"
    Write-Warning "This will delete all Docker resources related to tweetoscope!"
    
    $response = Read-Host "Are you sure? (y/N)"
    if ($response -eq 'y' -or $response -eq 'Y') {
        Write-Header "Stopping services..."
        docker-compose down -v 2>$null
        
        Write-Header "Removing images..."
        $images = docker images 'tweetoscope*' -q 2>$null
        if ($images) {
            docker rmi $images 2>$null
        }
        
        Write-Success "Cleanup completed"
    } else {
        Write-Warning "Operation cancelled"
    }
}

# Rebuild everything
function Rebuild {
    param([string]$RegistryUser)
    
    Write-Header "Full rebuild: clean → build → up"
    
    Clean-All
    Build-Image $RegistryUser
    Start-Services
}

# Main command dispatcher
switch ($Command) {
    'build'          { Build-Image $Argument }
    'push'           { Push-Image $Argument }
    'up'             { Start-Services }
    'down'           { Stop-Services }
    'down-v'         { Stop-Services-Volumes }
    'logs'           { Show-Logs $Argument }
    'ps'             { Show-Status }
    'restart'        { Restart-Services $Argument }
    'shell'          { Open-Shell $Argument }
    'kafka-topics'   { List-Topics }
    'kafka-describe' { Describe-Topic $Argument }
    'kafka-consume'  { Consume-Topic $Argument }
    'kafka-produce'  { Produce-Topic $Argument }
    'clean'          { Clean-All }
    'rebuild'        { Rebuild $Argument }
    'help'           { Show-Help }
    default {
        Write-Error "Unknown command: $Command"
        Write-Host ""
        Show-Help
        exit 1
    }
}
