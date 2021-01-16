package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

// thanks for this amazing content
// https://medium.com/swlh/unit-testing-cli-programs-in-go-6275c85af2e7
var out io.Writer = os.Stdout

// go test -v -run ^Test_Main$ -count 10
func Test_Main(t *testing.T) {
	os.Args = []string{"./func.go", "Funcionarios-10K.json"}
	out = bytes.NewBuffer(nil)
	main()

}
