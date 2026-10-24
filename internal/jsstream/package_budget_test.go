package jsstream

import (
	"go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Measure every physical platform twin, using the central structural gate's
// token-line definition. A Linux-only build count hid the sixth path file;
// comments and blank lines never count toward the frozen code budget.
// origin: S7 internal review FIX-ARCH F2 / rv-wiring F9
func TestTransferPackagesStayWithinFrozenCaps(t *testing.T) {
	for _, cap := range []struct {
		pkg         string
		files, code int
	}{
		{"jsstream", 9, 2099},
		{"transferpath", 5, 1180},
		{"xferbucket", 3, 1800},
	} {
		t.Run(cap.pkg, func(t *testing.T) {
			dir := filepath.Join("..", cap.pkg)
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			files, code := 0, 0
			for _, entry := range entries {
				name := entry.Name()
				if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
					continue
				}
				path := filepath.Join(dir, name)
				src, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				fset := token.NewFileSet()
				file := fset.AddFile(path, fset.Base(), len(src))
				var scan scanner.Scanner
				scan.Init(file, src, func(pos token.Position, msg string) {
					t.Fatalf("scan %s: %s", pos, msg)
				}, 0)
				lines := map[int]bool{}
				for {
					pos, tok, _ := scan.Scan()
					if tok == token.EOF {
						break
					}
					if tok != token.SEMICOLON {
						lines[file.Position(pos).Line] = true
					}
				}
				files++
				code += len(lines)
			}
			t.Logf("%s: AllOS files=%d/%d stripped code=%d/<%d", cap.pkg, files, cap.files, code, cap.code)
			if files == 0 || code == 0 || files > cap.files || code >= cap.code {
				t.Fatalf("%s exceeds its frozen package cap", cap.pkg)
			}
		})
	}
}
