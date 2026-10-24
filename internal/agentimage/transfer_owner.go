package agentimage

import "github.com/LinZiyang666/tether/internal/agentimage/transfercontract"

// Keep the composition's public ports identical to the neutral leaf's ports:
// adapters must not import this composition and inherit unrelated runtimes.
// origin: S7 internal review IMAGE-LEAF F1
type (
	TransferWorker     = transfercontract.TransferWorker
	TransferFrozen     = transfercontract.TransferFrozen
	TransferImporter   = transfercontract.TransferImporter
	TransferFileSource = transfercontract.TransferFileSource
	TransferState      = transfercontract.TransferState
)
