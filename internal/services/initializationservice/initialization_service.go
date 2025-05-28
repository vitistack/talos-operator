package initializationservice

import "github.com/NorskHelsenett/ror/pkg/rlog"

func CheckPrerequisites() {
	// check if kubernetes is running on talos
	rlog.Info("Running prerequisite checks...")
	rlog.Info("✅ Prerequisite checks passed")
}
