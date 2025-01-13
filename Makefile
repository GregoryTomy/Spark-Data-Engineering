build:
	docker compose build spark-master

docker-up:
	docker compose up --build -d --scale spark-worker=2

up: build docker-up

down:
	docker compose down -v --remove-orphans

fake-datagen:
	docker exec spark-master bash -c "python3 /opt/spark/work-dir/datagen/datagen.py"

create-buckets:
	docker exec spark-master bash -c "python3 /opt/spark/work-dir/create_buckets.py"

setup: fake-datagen create-buckets
	@echo "Setup complete!"

reset: down up

bash:
	docker exec -it spark-master bash

project:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./run_etl.py

mypy:
	docker exec spark-master mypy --no-implicit-reexport --ignore-missing-imports --no-namespace-packages .
