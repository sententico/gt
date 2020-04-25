package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

var (
	csvMapFlag string
	txtMapFlag string
	detailFlag bool
	wg         sync.WaitGroup
)

func init() {
	flag.BoolVar(&detailFlag, "d", false, fmt.Sprintf("specify detailed output"))
	flag.StringVar(&csvMapFlag, "csv", "", fmt.Sprintf("specify CSV column map: \"<head>:<col>,...\""))
	flag.StringVar(&txtMapFlag, "txt", "", fmt.Sprintf("specify TXT column map: \"<head>:<a>:<b>,...\""))

	flag.Usage = func() {
		fmt.Printf("command usage: gt <flags>\n")
		flag.PrintDefaults()
	}
}

func peekRead(path string) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("%v\n", e)
		}
	}()
	dig, e := PeekCSV(path)
	if e != nil {
		panic(fmt.Errorf("%v", e))
	}

	fmt.Println(dig)
	var (
		in  <-chan map[string]string
		err <-chan error
		sig chan<- int
	)
	switch dig.sep {
	case '\x00':
		in, err, sig = ReadTXT(path, func() (m map[string][2]int) {
			if txtMapFlag == "" {
				return map[string][2]int{"~raw": {1, len(dig.preview[0])}}
			}
			m = make(map[string][2]int, 32)
			a, b, p := 0, 0, 0
			for _, t := range strings.Split(txtMapFlag, ",") {
				switch v := strings.Split(t, ":"); len(v) {
				case 1:
					if p < len(dig.preview[0]) {
						m[strings.Trim(v[0], " ")] = [2]int{p + 1, len(dig.preview[0])}
					}
					continue
				case 2:
					if b, _ = strconv.Atoi(v[1]); b > p {
						m[strings.Trim(v[0], " ")] = [2]int{p + 1, b}
					}
				default:
					a, _ = strconv.Atoi(v[1])
					b, _ = strconv.Atoi(v[2])
					if a > 0 && b > a {
						m[strings.Trim(v[0], " ")] = [2]int{a, b}
					}
				}
				p = b
			}
			return
		}(), dig.comment)
	default:
		in, err, sig = ReadCSV(path, func() (m map[string]int) {
			if csvMapFlag != "" {
				m = make(map[string]int, 32)
				for i, t := range strings.Split(csvMapFlag, ",") {
					switch v := strings.Split(t, ":"); len(v) {
					case 1:
						m[strings.Trim(v[0], " ")] = i + 1
					default:
						if c, _ := strconv.Atoi(v[1]); c > 0 {
							m[strings.Trim(v[0], " ")] = c
						}
					}
				}
			}
			return
		}(), dig.sep, dig.comment)
	}
	defer close(sig)

	rows := 0
	for row := range in {
		if rows++; detailFlag {
			fmt.Println(row)
		}
	}
	if e := <-err; e != nil {
		panic(fmt.Errorf("%v", e))
	}
	fmt.Printf("read %d rows from %q\n", rows, path)
}

func main() {
	flag.Parse()
	for _, file := range flag.Args() {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			peekRead(f)
		}(file)
	}
	wg.Wait()
}
