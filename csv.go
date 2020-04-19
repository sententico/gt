package main

import (
	"bufio"
	"os"
	"strings"
)

func PeekText(path string, lines int, comment string) []string {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	var peek []string
	for lnc, ln := 0, bufio.NewScanner(file); lnc < lines && ln.Scan(); {
		switch s := ln.Text(); {
		case comment != "" && (len(s) == 0 || strings.HasPrefix(s, comment)):
		default:
			peek = append(peek, s)
			lnc++
		}
	}
	return peek
}

func GetText(path string) <-chan string {
	in := make(chan string, 256)
	go func() {
		defer close(in)
		file, err := os.Open(path)
		if err != nil {
			return
		}
		defer file.Close()
		for ln := bufio.NewScanner(file); ln.Scan(); in <- ln.Text() {
		}
	}()
	return in
}

func GetCSV(path string, heads []string, sep string, comment string) <-chan map[string]string {
	in := make(chan map[string]string, 256)
	go func() {
		defer close(in)
		for ln := range GetText(path) {
			for {
				switch {
				case comment != "" && (len(ln) == 0 || strings.HasPrefix(ln, comment)):
				case sep == "" && len(ln) > 0:
					max := 0
					for _, s := range []string{",", "\t", "|", ";", ":"} {
						if c := len(strings.Split(ln, s)); c == len(heads) {
							sep = s
							break
						} else if c > max {
							max, sep = c, s
						}
					}
					continue
				case heads == nil && len(ln) > 0:
					for _, v := range strings.Split(ln, sep) {
						heads = append(heads, strings.Trim(v, " \""))
					}
				default:
					if s := strings.Split(ln, sep); len(s) == len(heads) {
						m := make(map[string]string, len(s))
						for i, v := range s {
							m[heads[i]] = strings.Trim(v, " \"")
						}
						in <- m
					}
				}
				break
			}
		}
	}()
	return in
}
