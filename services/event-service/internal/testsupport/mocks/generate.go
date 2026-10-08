package mocks

//go:generate go run go.uber.org/mock/mockgen -source=../../platform/port/repository.go -destination=repository.go -package=mocks
//go:generate go run go.uber.org/mock/mockgen -source=../../adapter/messaging/kafka/dlq.go -destination=deadletters.go -package=mocks
