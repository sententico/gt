package main

import (
	"fmt"
	"math"
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
	n, m, r, s := 10000000000000.0, 8.0, make(chan float64), 0.0
	for i := 0.0; i < m; i++ {
		go series(n-i, m, r)
	}
	for i := 0.0; i < m; i++ {
		s += <-r
	}
	fmt.Println(s - math.Log(n))
}
