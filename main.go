package main

import "fmt"

func main() {
	t := &test{3}
	fix(t)
	fmt.Println(t)
}

func fix(t *test) {
	t.id = 1
}

type test struct {
	id int
}
