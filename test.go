package main

import (
	"fmt"
)

func main() {
	in, err, sig := ReadCSV("test.csv", map[string]int{"animal": 1, "value": 2}, '\x00', "#")
	//in, err, sig := ReadTXT("test.txt", map[string][]int{"animal": {1, 15}, "value": {26, 32}}, "//")
	defer close(sig)
	for csv := range in {
		fmt.Println(csv)
	}
	if e := <-err; e != nil {
		fmt.Println(e)
	}

	if dig, e := PeekCSV("test.csv"); e != nil {
		fmt.Println(e)
	} else {
		fmt.Println(dig)
	}
}
