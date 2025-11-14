ssh hrexpense_user@212.95.35.168

cd /var/www/po-app-backend
docker compose up --build -d
docker compose down && docker compose up -d
docker compose ps

docker compose logs -f --tail=100 backend
