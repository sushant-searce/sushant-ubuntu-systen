.ONESHELL:
.PHONY:all
all:
	@echo "make build -- generate .env file for the following operation"
	@echo "make start | stop -- start or stop the service"
	@echo "make destroy -- stop and remove the service from the machine"
	@echo "make clean" -- remove the .env file

.PHONY: start
start: check_env_file
	sudo docker-compose up -d --remove-orphans --force-recreate
	@echo "Started"

.PHONY: stop
stop: check_env_file
	sudo docker-compose stop
	@echo "Stopped"

.PHONY: restart
restart: check_env_file
	sudo docker-compose restart
	@echo "Restarted"

.PHONY: destroy
destroy: check_env_file
	sudo docker-compose rm --stop --force
	@echo "Destroyed"

.PHONY: clean
clean:
	rm -rf .env

.PHONY: build
build: check_env_variables
ifeq ($(wildcard .env),)
	@echo "APP_PROFILE=${APP_PROFILE}" >> .env
	@echo "APP_HOST=${APP_HOST}" >> .env
endif

.PHONY: check_env_file
check_env_file:
ifeq ($(wildcard .env),)
	$(error .env file not found, you can run "APP_PROFILE=? APP_HOST=? make build" create it)
endif

.PHONY: check_env_variables
check_env_variables:
ifeq ($(APP_PROFILE),)
	$(error APP_PROFILE is undefined)
endif
