//go:build integration || component || contract || e2e

package tests

import (
	"os"
	"testing"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/tests/common"
)

func TestMain(m *testing.M) {
	os.Exit(common.Run(m))
}
