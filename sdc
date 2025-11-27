ssh hrexpense_user@212.95.35.168

cd /var/www/po-app-backend
git pull origin main
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


DELETE FROM merged_pos;
DELETE FROM raw_purchase_orders;
DELETE FROM raw_acceptances;
DELETE FROM sites;
DELETE FROM customer_projects;
DELETE FROM internal_projects;


# 1. Go to project folder
cd /var/www/po-app-backend

# 2. Enter the container
docker compose exec backend bash


# 7. (Inside container) Generate the migration script
alembic revision --autogenerate -m "Add acceptance columns"

# 8. (Inside container) Apply the migration
alembic upgrade head

# 9. (Inside container) Exit for the last time
exit

# 10. Restart the application service
docker compose restart backend