#!/bin/sh
docker image rm  airbnb_concierge_db_feed:test
docker build 	--tag airbnb_concierge_db_feed:test \
	-f ./docker_images/airflow/Dockerfile .
