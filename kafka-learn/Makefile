.PHONY: up down logs topics clean

up:
	docker compose up -d
	@echo "waiting for the broker to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done
	@echo "broker is up. Kafka UI: http://localhost:8080  Schema Registry: http://localhost:8081"

down:
	docker compose down

logs:
	docker compose logs -f kafka

# creates every topic used across all six stages -- safe to re-run, --if-not-exists no-ops
topics:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.v1            --partitions 1 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.keyed         --partitions 3 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.groups        --partitions 4 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.delivery      --partitions 3 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.idempotent    --partitions 3 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic revenue.v1           --partitions 3 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.avro          --partitions 3 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.dirty         --partitions 1 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders.dirty.dlq     --partitions 1 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic revenue.windowed     --partitions 1 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

clean: down
	docker volume prune -f --filter label=com.docker.compose.project=kafka-learn
