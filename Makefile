include .env

build: # Build the docker images
	docker-compose build

up: # Start the docker containers
	docker-compose up -d

down: # Stop the docker containers
	docker-compose down

restart: # Restart the docker containers
	make down && make up

to_psql: # Connect to the PostgreSQL container
	docker exec -ti de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

to_mysql: # Connect to the MySQL container
	docker exec -it de_mysql mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

to_mysql_root: # Connect to the MySQL container as root
	docker exec -it de_mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}