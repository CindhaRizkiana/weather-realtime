include .env

help:
	@echo "## docker-build			- Build Docker Images (amd64) including its inter-container network."
	@echo "## docker-build-arm		- Build Docker Images (arm64) including its inter-container network."
	@echo "## postgres				- Run a Postgres container  "
	@echo "## spark					- Run a Spark cluster, rebuild the postgres container, then create the destination tables "
	@echo "## jupyter				- Spinup jupyter notebook for testing and validation purposes."
	@echo "## kafka					- Spinup kafka cluster (Kafka+Zookeeper)."
	@echo "## datahub				- Spinup datahub instances."
	@echo "## metabase				- Spinup metabase instance."
	@echo "## cassandra				- Spinup Cassandra container."
	@echo "## clean					- Cleanup all running containers related to the challenge."

docker-build:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@docker network inspect dataeng-network >/dev/null 2>&1 || docker network create dataeng-network
	@echo '__________________________________________________________'
	@docker build -t dataeng/spark -f ./docker/Dockerfile.spark .
	@echo '__________________________________________________________'

docker-build-arm:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@docker network inspect dataeng-network >/dev/null 2>&1 || docker network create dataeng-network
	@echo '__________________________________________________________'
	@docker build -t dataeng/spark -f ./docker/Dockerfile.spark .
	@echo '__________________________________________________________'

spark:
	@echo '__________________________________________________________'
	@echo 'Creating Spark Cluster ...'
	@echo '__________________________________________________________'
	@docker-compose -f ./docker/docker-compose-spark.yml --env-file .env up -d
	@echo '==========================================================='


kafka: kafka-create

kafka-create:
	@echo '__________________________________________________________'
	@echo 'Creating Kafka Cluster ...'
	@echo '__________________________________________________________'
	@docker-compose -f ./docker/docker-compose-kafka.yml --env-file .env up -d
	@echo 'Waiting for uptime on http://localhost:8083 ...'
	@sleep 20
	@echo '==========================================================='

kafka-create-test-topic:
	@docker exec ${KAFKA_CONTAINER_NAME} \
		kafka-topics.sh --create \
		--partitions 3 \
		--replication-factor ${KAFKA_REPLICATION} \
		--bootstrap-server localhost:9092 \
		--topic ${KAFKA_TOPIC_NAME}

kafka-create-topic:
	@docker exec ${KAFKA_CONTAINER_NAME} \
		kafka-topics.sh --create \
		--partitions ${partition} \
		--replication-factor ${KAFKA_REPLICATION} \
		--bootstrap-server localhost:9092 \
		--topic ${topic}

cassandra: cassandra-create-keyspace

cassandra-create-keyspace:
	@echo '__________________________________________________________'
	@echo 'Creating Cassandra Keyspace ...'
	@echo '__________________________________________________________'
	@docker-compose -f ./docker/docker-compose-cassandra.yml --env-file .env up -d
	@echo 'Waiting for Cassandra to initialize...'
	@sleep 30  # Adjust the sleep duration as needed
	@echo '==========================================================='

spark-produce:
	@echo '__________________________________________________________'
	@echo 'Producing data ...'
	@echo '__________________________________________________________'
	@docker exec ${SPARK_WORKER_CONTAINER_NAME}-1 \
		python \
		/scripts/kafka_streams.py

spark-consume:
	@echo '__________________________________________________________'
	@echo 'Producing fake events ...'
	@echo '__________________________________________________________'
	@docker exec ${SPARK_WORKER_CONTAINER_NAME}-1 \
		spark-submit \
		/spark-scripts/spark_streams.py

clean:
	@bash ./scripts/goodnight.sh