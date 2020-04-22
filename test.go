package main

import (
	"fmt"
	//"time"
	//"math"
)

func series(n float64, m float64, r chan float64) {
	s := 0.0
	for i := n; i > 0.0; i -= m {
		s += 1.0 / i
	}
	r <- s
}

func fib(n int) int {
	if n < 2 {
		return 1
	}
	return fib(n-2) + fib(n-1)
}

func main() {
	in, sig := ReadCSV("test.csv", map[string]int{"animal": 1, "value": 2}, '\x00', "#")
	//in, sig := ReadTXT("test.txt", map[string][2]int{"animal": {1, 15}, "value": {26, 32}}, "//"
	defer close(sig)
	for csv := range in {
		fmt.Println(csv)
	}
	fmt.Println(PeekCSV("test.csv"))

	// n, m, r, s := 1e10, 8.0, ake(chan float64), 0.0
	// for i := .0; i < m; ++ {
	// 	 series(n-i, m, r)
	// }
	// fr i := 00; i < m; i++ {
	// 	 += <-r
	// }
	// ft.Println(s - math.Log(n))
}
