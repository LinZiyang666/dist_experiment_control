package architecture

import (
	"strconv"
	"testing"
)

// Copy into test/architecture. Uses the existing AllOS source scanner only;
// no nested Go builds, Darwin compilation or full gate suite is invoked.
// origin: S7 internal review rv-wiring F9
func TestTransferCurrentStructuralCaps(t *testing.T) {
	result := measureBrokerStructure(t, brokerGateSources(t, repoRoot(t)))
	for pkg, cap := range transferPackageCaps {
		files, code := result.files["internal/"+pkg], result.code["internal/"+pkg]
		if pkg == "xfercontract/xfervalue" || pkg == "xferauth" || pkg == "xfercontrol" {
			t.Logf("%s: files=%d/%d code=%d/<%d", pkg, files, cap.files, code, cap.code)
		}
		if files > cap.files || code >= cap.code {
			t.Errorf("%s: files=%d (cap %d), stripped code=%d (cap <%d)", pkg, files, cap.files, code, cap.code)
		}
	}
	// Existing-package registrations are frozen separately from the new leaves.
	// Omitting them let this counterexample pass while both JS and path owners
	// exceeded their AllOS caps; use the same rows as the production gate.
	// origin: S7 internal review FIX-ARCH F2 / rv-wiring F9
	rows, err := readTransferTSV(repoRoot(t), "test/architecture/testdata/transfer_source_moves.tsv#existing-package-caps", 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		t.Run(row[0], func(t *testing.T) {
			files, fileErr := strconv.Atoi(row[1])
			code, codeErr := strconv.Atoi(row[2])
			t.Logf("%s: files=%d/%d code=%d/<%d", row[0], result.files[row[0]], files, result.code[row[0]], code)
			if fileErr != nil || codeErr != nil || files <= 0 || code <= 0 || result.files[row[0]] == 0 ||
				result.files[row[0]] > files || result.code[row[0]] >= code {
				t.Fatal("AllOS first-registration file/code cap exceeded or empty")
			}
		})
	}
}
