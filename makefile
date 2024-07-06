# Push to main quicker

DATE := $(shell date +%Y-%m-%d)

all: add commit push

add:
	git add .

commit:
	@read -p "Please enter an additional commit message: " msg; \
	git commit -m "updates $(DATE) - $$msg"

push:
	git push
