docker builder prune --all --force
docker-compose down 
docker rmi -f $(docker images -aq)
export AIRFLOW_UID=5000 && docker-compose up --build -d
