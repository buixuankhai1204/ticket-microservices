package usecase_test

import (
	"github.com/jackc/pgx/v5"
	"go.uber.org/mock/gomock"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/testsupport"
)

func inTx(db *testsupport.FakeDB) gomock.Matcher {
	return gomock.Cond(func(tx pgx.Tx) bool { return db.IsCurrentTx(tx) })
}

func lastCall(db *testsupport.FakeDB) string {
	calls := db.Calls()
	if len(calls) == 0 {
		return ""
	}
	return calls[len(calls)-1]
}
