machine ?= docker

%-up:
	$(machine) compose --profile $* up -d

%-up-build:
	$(machine) compose --profile $* up -d --build

%-down:
	$(machine) compose --profile $* down
