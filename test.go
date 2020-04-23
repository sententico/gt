package main

import (
	"flag"
	"fmt"
)

func printTXT(path string, cols map[string][2]int, comment string) {
	in, err, sig := ReadTXT(path, cols, comment)
	defer close(sig)
	for txt := range in {
		fmt.Println(txt)
	}
	if e := <-err; e != nil {
		fmt.Println(e)
	}
}

func printCSV(path string, sep rune, comment string) {
	in, err, sig := ReadCSV(path, nil, sep, comment)
	defer close(sig)
	for csv := range in {
		fmt.Println(csv)
	}
	if e := <-err; e != nil {
		fmt.Println(e)
	}
}

func peekPrint(path string) {
	if dig, e := PeekCSV(path); e != nil {
		fmt.Println(e)
	} else if dig.sep > '\x00' {
		printCSV(path, dig.sep, dig.comment)
	} else {
		printTXT(path, map[string][2]int{"~raw": {1, len(dig.raw[0])}}, dig.comment)
	}
}

func main() {
	path := flag.String("p", "test.txt", fmt.Sprintf("specify `pathname`"))
	flag.Usage = func() {
		fmt.Printf("command usage: gt <flags>\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	peekPrint(*path)
}
