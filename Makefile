.PHONY:	fmt
fmt:
	@sh ./.script/goimports.sh

.PHONY: tidy
tidy:
	@go mod tidy -v