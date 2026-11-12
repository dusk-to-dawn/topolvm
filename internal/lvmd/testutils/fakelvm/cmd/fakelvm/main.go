// Command fakelvm is the fake lvm command used by lvmd's tests.
package main

import (
	"os"

	"github.com/topolvm/topolvm/internal/lvmd/testutils/fakelvm"
)

func main() {
	os.Exit(fakelvm.Main(os.Args[1:]))
}
