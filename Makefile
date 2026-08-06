GOLANG := go

GO_TEST_PATH ?= ./...
GO_TEST_FLAGS ?= -v -race -count 1
GO_TEST_TIMEOUT ?= 30s
GO_TEST_RUN ?= .

.PHONY: test
test:
	${GOLANG} test ${GO_TEST_FLAGS} -run ${GO_TEST_RUN} -timeout ${GO_TEST_TIMEOUT} ${GO_TEST_PATH}
