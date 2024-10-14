# Make the deployment of updated docker containers quick and easy in the future.

# Docker and AWS section
.PHONY: docker-login docker-build docker-tag docker-push docker-verify docker-all

docker-all: docker-login docker-build docker-tag docker-push docker-verify

docker-login:
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com

docker-build:
	docker build -t $(REPO_NAME) .

docker-tag:
	docker tag $(REPO_NAME):latest $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(REPO_NAME):latest

docker-push:
	docker push $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(REPO_NAME):latest

docker-verify:
	aws ecr describe-images --repository-name $(REPO_NAME) --region $(REGION)

# Git section
.PHONY: git-all git-add git-commit git-push

DATE := $(shell date +%Y-%m-%d)

git-all: git-add git-commit git-push

git-add:
	git add .

git-commit:
	@read -p "Please enter an additional commit message: " msg; \
	git commit -m "updates $(DATE) - $$msg"

git-push:
	git push

# Local Dagster Dev Section
# Make sure .venv is active
# Make sure you have set the correct variables in the .env file
# Sometimes you have to get rid of the dagster.yaml files for this to work
.PHONY: dagster-dev
dagster-dev:
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found"; \
		exit 1; \
	fi
	set -a && source .env && set +a && \
	dagster dev -m analytics_platform_dagster

# Run Streamlit Data Lake Checker Locally
# Make sure .venv is active and you have run aws-vault exec <profile>
.PHONY: run-st
run-st:
	cd streamlit_app && streamlit run main.py
