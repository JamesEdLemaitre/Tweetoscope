#!/bin/bash

# Script d'aide pour gérer l'architecture Tweetoscope Kafka avec Docker

set -e

REGISTRY_USER="${1:-}"
IMAGE_NAME="tweetoscope-kafka"
IMAGE_TAG="latest"

# Couleurs pour l'output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonctions d'affichage
print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Affiche l'aide
show_help() {
    cat << EOF
Usage: ./docker-manage.sh <command> [options]

Commands:
    build [registry_user]      Build Docker image
                               Exemple: ./docker-manage.sh build thierry
    
    push [registry_user]       Push image to Docker Hub (requires 'build' first)
                               Exemple: ./docker-manage.sh push thierry
    
    up                         Start all services with docker-compose
    
    down                        Stop all services
    
    down-v                      Stop all services and remove volumes
    
    logs [service]             Show logs from all services or specific service
                               Services: producer, filter, extractor, counter, leaderboard, kafka
                               Exemple: ./docker-manage.sh logs producer
    
    ps                         Show status of all services
    
    restart [service]          Restart all services or specific service
                               Exemple: ./docker-manage.sh restart producer
    
    shell <service>            Open shell in specific service container
                               Exemple: ./docker-manage.sh shell kafka
    
    kafka-topics              List all Kafka topics
    
    kafka-describe <topic>     Describe a specific Kafka topic
                               Exemple: ./docker-manage.sh kafka-describe raw-tweets
    
    kafka-consume <topic>      Consume messages from a topic (from beginning)
                               Exemple: ./docker-manage.sh kafka-consume raw-tweets
    
    kafka-produce <topic>      Produce messages to a topic (interactive)
                               Exemple: ./docker-manage.sh kafka-produce raw-tweets
    
    clean                      Remove all containers, images and volumes
    
    rebuild [registry_user]    Clean, build and restart
                               Exemple: ./docker-manage.sh rebuild thierry
    
    help                       Show this help message

EOF
}

# Build Docker image
build_image() {
    local user="$1"
    
    if [ -z "$user" ]; then
        print_error "Registry user not specified"
        echo "Usage: ./docker-manage.sh build <registry_user>"
        echo "Example: ./docker-manage.sh build thierry"
        return 1
    fi
    
    print_header "Building Docker image: $user/$IMAGE_NAME:$IMAGE_TAG"
    
    if docker build -t "$user/$IMAGE_NAME:$IMAGE_TAG" .; then
        print_success "Image built successfully: $user/$IMAGE_NAME:$IMAGE_TAG"
    else
        print_error "Build failed"
        return 1
    fi
}

# Push to Docker Hub
push_image() {
    local user="$1"
    
    if [ -z "$user" ]; then
        print_error "Registry user not specified"
        echo "Usage: ./docker-manage.sh push <registry_user>"
        return 1
    fi
    
    print_header "Pushing image to Docker Hub: $user/$IMAGE_NAME:$IMAGE_TAG"
    
    if docker push "$user/$IMAGE_NAME:$IMAGE_TAG"; then
        print_success "Image pushed successfully"
    else
        print_error "Push failed"
        return 1
    fi
}

# Start services
up_services() {
    print_header "Starting all services with docker-compose"
    
    if docker-compose up -d; then
        print_success "All services started"
        print_header "Service Status"
        docker-compose ps
    else
        print_error "Failed to start services"
        return 1
    fi
}

# Stop services
down_services() {
    print_header "Stopping all services"
    
    if docker-compose down; then
        print_success "All services stopped"
    else
        print_error "Failed to stop services"
        return 1
    fi
}

# Stop services and remove volumes
down_volumes() {
    print_header "Stopping all services and removing volumes"
    print_warning "This will delete all data!"
    
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if docker-compose down -v; then
            print_success "All services stopped and volumes removed"
        else
            print_error "Failed to stop services"
            return 1
        fi
    else
        print_warning "Operation cancelled"
    fi
}

# Show logs
show_logs() {
    local service="$1"
    
    if [ -z "$service" ]; then
        print_header "Showing logs from all services"
        docker-compose logs -f
    else
        case "$service" in
            producer) service="tweetoscope-producer" ;;
            filter) service="tweetoscope-filter" ;;
            extractor) service="tweetoscope-extractor" ;;
            counter) service="tweetoscope-counter" ;;
            leaderboard) service="tweetoscope-leaderboard" ;;
            kafka) service="kafka" ;;
            *)
                print_error "Unknown service: $service"
                echo "Valid services: producer, filter, extractor, counter, leaderboard, kafka"
                return 1
                ;;
        esac
        
        print_header "Showing logs from service: $service"
        docker-compose logs -f "$service"
    fi
}

# Show status
show_status() {
    print_header "Service Status"
    docker-compose ps
}

# Restart services
restart_services() {
    local service="$1"
    
    if [ -z "$service" ]; then
        print_header "Restarting all services"
        docker-compose restart
    else
        case "$service" in
            producer) service="tweetoscope-producer" ;;
            filter) service="tweetoscope-filter" ;;
            extractor) service="tweetoscope-extractor" ;;
            counter) service="tweetoscope-counter" ;;
            leaderboard) service="tweetoscope-leaderboard" ;;
            kafka) service="kafka" ;;
            *)
                print_error "Unknown service: $service"
                echo "Valid services: producer, filter, extractor, counter, leaderboard, kafka"
                return 1
                ;;
        esac
        
        print_header "Restarting service: $service"
        docker-compose restart "$service"
    fi
    
    print_success "Restart completed"
}

# Open shell in container
open_shell() {
    local service="$1"
    
    if [ -z "$service" ]; then
        print_error "Service not specified"
        echo "Usage: ./docker-manage.sh shell <service>"
        return 1
    fi
    
    case "$service" in
        producer) service="tweetoscope-producer" ;;
        filter) service="tweetoscope-filter" ;;
        extractor) service="tweetoscope-extractor" ;;
        counter) service="tweetoscope-counter" ;;
        leaderboard) service="tweetoscope-leaderboard" ;;
        kafka) service="kafka" ;;
        *)
            print_error "Unknown service: $service"
            echo "Valid services: producer, filter, extractor, counter, leaderboard, kafka"
            return 1
            ;;
    esac
    
    print_header "Opening shell in service: $service"
    docker-compose exec "$service" bash || docker-compose exec "$service" sh
}

# Kafka topics
list_topics() {
    print_header "Kafka Topics"
    docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
}

# Describe Kafka topic
describe_topic() {
    local topic="$1"
    
    if [ -z "$topic" ]; then
        print_error "Topic not specified"
        echo "Usage: ./docker-manage.sh kafka-describe <topic>"
        return 1
    fi
    
    print_header "Topic Description: $topic"
    docker exec kafka kafka-topics.sh --describe --topic "$topic" --bootstrap-server localhost:9092
}

# Consume from Kafka topic
consume_topic() {
    local topic="$1"
    
    if [ -z "$topic" ]; then
        print_error "Topic not specified"
        echo "Usage: ./docker-manage.sh kafka-consume <topic>"
        return 1
    fi
    
    print_header "Consuming from topic: $topic (Press Ctrl+C to stop)"
    docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$topic" --from-beginning
}

# Produce to Kafka topic
produce_topic() {
    local topic="$1"
    
    if [ -z "$topic" ]; then
        print_error "Topic not specified"
        echo "Usage: ./docker-manage.sh kafka-produce <topic>"
        return 1
    fi
    
    print_header "Producing to topic: $topic (Press Ctrl+C to stop)"
    docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$topic"
}

# Clean everything
clean_all() {
    print_header "Cleaning all containers, images and volumes"
    print_warning "This will delete all Docker resources related to tweetoscope!"
    
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_header "Stopping services..."
        docker-compose down -v 2>/dev/null || true
        
        print_header "Removing images..."
        docker rmi $(docker images 'tweetoscope*' -q) 2>/dev/null || true
        docker rmi $(docker images 'kafka*' -q) 2>/dev/null || true
        
        print_success "Cleanup completed"
    else
        print_warning "Operation cancelled"
    fi
}

# Rebuild everything
rebuild() {
    local user="$1"
    
    print_header "Full rebuild: clean → build → up"
    
    clean_all
    build_image "$user"
    up_services
}

# Main command handler
case "${1:-help}" in
    build) build_image "$2" ;;
    push) push_image "$2" ;;
    up) up_services ;;
    down) down_services ;;
    down-v) down_volumes ;;
    logs) show_logs "$2" ;;
    ps) show_status ;;
    restart) restart_services "$2" ;;
    shell) open_shell "$2" ;;
    kafka-topics) list_topics ;;
    kafka-describe) describe_topic "$2" ;;
    kafka-consume) consume_topic "$2" ;;
    kafka-produce) produce_topic "$2" ;;
    clean) clean_all ;;
    rebuild) rebuild "$2" ;;
    help) show_help ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
