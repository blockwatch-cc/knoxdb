// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package containers

func RegisterAll() {
	// Register all known containers here
	RegisterTransferContainer()
	RegisterAccountContainer()
}

func RegisterTransferContainer() {
	_ = TransferContainer{} // force import
}

func RegisterAccountContainer() {
	_ = AccountContainer{} // force import
}
