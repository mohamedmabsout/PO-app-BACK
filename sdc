ssh hrexpense_user@212.95.35.168

cd /var/www/po-app-backend
docker compose up --build -d
docker compose down && docker compose up -d
docker compose ps

docker compose logs -f --tail=100 backend




# On the server, in your project directory

# Step 1: Stop the running containers
docker compose down

# Step 2: Rebuild the image and start the services again
# The --build flag is critical here
docker compose up -d --build

# Step 3: Now that the container is running with the corrected file,
# execute the migration.
docker compose exec backend alembic upgrade head

alembic revision --autogenerate -m "..."
alembic revision --autogenerate -m "project and site linking"

alembic revision --autogenerate -m "project and site linking"


DELETE FROM merged_pos;
DELETE FROM raw_purchase_orders;
DELETE FROM raw_acceptances;
DELETE FROM sites;
DELETE FROM projects;