build:
	docker compose build spark-master

docker-up:
	docker compose up --build -d --scale spark-worker=2

up: build docker-up

down:
	docker compose down
