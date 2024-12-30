build:
	docker compose build spark-master

docker-up:
	docker compose up --build -d --scale spark-worker=2

up: build docker-up

down:
	docker compose down

fake-datagen:
	docker exec spark-master bash -c "python3 /opt/spark/work-dir/datagen/datagen.py"

create-buckets:
	docker exec spark-master bash -c "python3 /opt/spark/work-dir/create_buckets.py"

setup: fake-datagen create-buckets
	@echo "Setup complete!"
