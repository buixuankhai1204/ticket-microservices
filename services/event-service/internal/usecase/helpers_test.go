package usecase_test

import (
	"github.com/jackc/pgx/v5"
	"go.uber.org/mock/gomock"
)

func inTx(db *FakeDB) gomock.Matcher {
	return gomock.Cond(func(tx pgx.Tx) bool { return db.IsCurrentTx(tx) })
}

func lastCall(db *FakeDB) string {
	calls := db.Calls()
	if len(calls) == 0 {
		return ""
	}
	return calls[len(calls)-1]
}
