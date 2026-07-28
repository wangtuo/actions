package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/example/actions-demo/pkg/greeter"
	"github.com/example/actions-demo/pkg/math"
)

// version 会在构建时通过 -ldflags 注入
var version = "dev"

func main() {
	name := flag.String("name", "world", "who to greet")
	a := flag.Int("a", 1, "first operand")
	b := flag.Int("b", 2, "second operand")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	fmt.Println(greeter.Hello(*name))
	fmt.Printf("%d + %d = %d\n", *a, *b, math.Add(*a, *b))
	os.Exit(0)
}
