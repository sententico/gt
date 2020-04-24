package main

import (
	"flag"
	"fmt"
	"sync"
)

var wg sync.WaitGroup

func printTXT(path string, cols map[string][2]int, comment string, detail bool) {
	in, err, sig := ReadTXT(path, cols, comment)
	defer close(sig)
	rows := 0
	for txt := range in {
		if rows++; detail {
			fmt.Println(txt)
		}
	}
	if e := <-err; e != nil {
		fmt.Println(e)
	}
	fmt.Printf("read %d rows from TXT file %q\n", rows, path)
}

func printCSV(path string, sep rune, comment string, detail bool) {
	in, err, sig := ReadCSV(path, nil, sep, comment)
	defer close(sig)
	rows := 0
	for csv := range in {
		if rows++; detail {
			fmt.Println(csv)
		}
	}
	if e := <-err; e != nil {
		fmt.Println(e)
	}
	fmt.Printf("read %d rows from CSV file %q\n", rows, path)
}

func peekPrint(path string, detail bool) {
	if dig, e := PeekCSV(path); e != nil {
		fmt.Println(e)
	} else if dig.sep > '\x00' {
		fmt.Println(dig)
		printCSV(path, dig.sep, dig.comment, detail)
	} else {
		fmt.Println(dig)
		printTXT(path, map[string][2]int{"~raw": {1, len(dig.preview[0])}}, dig.comment, detail)
	}
}

func main() {
	detail := flag.Bool("d", false, fmt.Sprintf("specify detailed output"))
	flag.Usage = func() {
		fmt.Printf("command usage: gt <flags>\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	for _, file := range flag.Args() {
		wg.Add(1)
		go func(f string, d bool) {
			defer wg.Done()
			peekPrint(f, d)
		}(file, *detail)
	}
	wg.Wait()
}
