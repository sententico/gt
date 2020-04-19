package main

import (
	"bufio"
	"os"
	"strings"
)

// PeekText returns up to "lines" lines of text from the file at "path" (or until EOF),
// skipping comments and blank lines if the comment prefix "comment" is specified.
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

// GetText returns a channel into which a goroutine writes lines of text from file at "path".
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

// GetCSV returns a channel into which a goroutine writes maps of CSV lines from file at "path"
// keyed by "heads" or if nil, by the heads in the first non-blank non-comment row.  CSV separator
// is "sep" unless blank; in which case, the separator will be inferred.  CSV heads and values are
// stripped of blanks and double-quotes.  If a prefix is specified in "comment", lines beginning
// with this prefix will be skipped.
func GetCSV(path string, heads []string, sep string, comment string) <-chan map[string]string {
	in := make(chan map[string]string, 256)
	go func() {
		defer close(in)
		for ln := range GetText(path) {
			tln := strings.TrimLeft(ln, " ")
			for {
				switch {
				case len(tln) == 0:
				case comment != "" && strings.HasPrefix(tln, comment):
				case sep == "":
					max := 0
					for _, s := range []string{",", "\t", "|", ";", ":"} {
						if c := len(strings.Split(tln, s)); c == len(heads) {
							sep = s
							break
						} else if c > max {
							max, sep = c, s
						}
					}
					continue
				case len(heads) == 0:
					for _, v := range strings.Split(tln, sep) {
						heads = append(heads, strings.Trim(v, " \""))
					}
				default:
					if s := strings.Split(tln, sep); len(s) == len(heads) {
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
