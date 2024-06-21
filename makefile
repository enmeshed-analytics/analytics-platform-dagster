# Push to main quicker

DATE := $(shell date +%Y-%m-%d)

all: add commit push

add:
	git add .

commit:
	git commit -m "Updates $(DATE)"

push:
	git push
