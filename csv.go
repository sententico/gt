package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Digest structure summarizing a CSV/TXT file for identification
type Digest struct {
	raw     []string
	comment string
	sep     rune
	split   []string
}

// handleSig is a goroutine that monitors the "sig" channel; when closed, "sigv" is modified
func handleSig(sig <-chan int, sigv *int) {
	*sigv = 0
	go func() {
		for *sigv = range sig {
		}
		*sigv = -1
	}()
}

// readLn returns a channel into which a goroutine writes lines from file at "path" (channels
// also provided for errors and for the caller to signal a halt).
func readLn(path string) (<-chan string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer close(out)
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
		}()
		file, e := os.Open(path)
		if e != nil {
			panic(fmt.Errorf("can't access %q (%v)", path, e))
		}
		defer file.Close()
		handleSig(sig, &sigv)

		ln := bufio.NewScanner(file)
		for ; sigv == 0 && ln.Scan(); out <- ln.Text() {
		}
		if e := ln.Err(); e != nil {
			panic(fmt.Errorf("problem reading %q (%v)", path, e))
		}
	}()
	return out, err, sig
}

// splitCSV returns a slice of fields in "ln" split by "sep", approximately following RFC 4180
func splitCSV(ln string, sep rune) []string {
	var fields []string
	field, encl := "", false
	for _, r := range ln {
		switch {
		case r == '"':
			encl = !encl
		case !encl && r == sep:
			fields = append(fields, field)
			field = ""
		case r < '\x20' || r > '\x7e':
			// alternatively replace non-printables: field += " "
		default:
			field += string(r)
		}
	}
	return append(fields, field)
}

// PeekCSV returns a digest to identify the CSV (or TXT file) at "path". This digest consists of a
// slice of the first few raw data lines (without blank or comment lines), the comment prefix used
// (if any), and if a CSV, the field separator with fields of the first data line split by it.
func PeekCSV(path string) (dig Digest, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	file, e := os.Open(path)
	if e != nil {
		panic(fmt.Errorf("can't access %q (%v)", path, e))
	}
	defer file.Close()
	bf, line, max := bufio.NewScanner(file), -1, 1
scan:
	for line < 4 && bf.Scan() {
		switch ln := bf.Text(); {
		case len(strings.TrimLeft(ln, " ")) == 0:
		case dig.comment != "" && strings.HasPrefix(ln, dig.comment):
		case line < 0:
			line = 0
			for _, p := range []string{"#", "//", "'"} {
				if strings.HasPrefix(ln, p) {
					dig.comment = p
					continue scan
				}
			}
			fallthrough
		default:
			line++
			dig.raw = append(dig.raw, ln)
		}
	}
	if e := bf.Err(); e != nil {
		panic(fmt.Errorf("problem reading %q (%v)", path, e))
	}
	for _, r := range ",\t|;:" {
		p, c := 0, 0
		for _, ln := range dig.raw {
			if c = len(splitCSV(ln, r)); c <= max || c != p && p > 0 {
				break
			}
			p = c
		}
		if c > max && c == p {
			max, dig.sep = c, r
		}
	}
	if dig.sep > '\x00' {
		for _, f := range splitCSV(dig.raw[0], dig.sep) {
			dig.split = append(dig.split, strings.Trim(f, " "))
		}
	}
	return
}

// ReadTXT returns a channel into which a goroutine writes maps of fixed-field TXT lines from file
// at "path" keyed by "heads" (channels also provided for errors and for the caller to signal a
// halt).  Fields selected by byte ranges in the "heads" map are stripped of blanks; blank lines
// and those prefixed by "comment" are skipped.
func ReadTXT(path string, heads map[string][2]int, comment string) (<-chan map[string]string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan map[string]string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer close(out)
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
		}()
		in, ierr, isig := readLn(path)
		defer close(isig)
		handleSig(sig, &sigv)

		vw, line, algn := 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case vw == 0:
					vw = len(ln)
					if len(heads) == 0 || len(heads) > vw {
						panic(fmt.Errorf("missing or bad head map provided for TXT file %q", path))
					}
					for _, r := range heads {
						if r[0] <= 0 || r[0] > r[1] || r[1] > vw {
							panic(fmt.Errorf("bad range in head map provided for TXT file %q", path))
						}
					}
					continue
				case len(ln) != vw:
					if algn++; line > 200 && float64(algn)/float64(line) > 0.05 {
						panic(fmt.Errorf("excessive column misalignment in TXT file %q (>%d rows)", path, algn))
					}
				default:
					m := make(map[string]string, len(heads))
					for h, r := range heads {
						m[h] = strings.Trim(ln[r[0]-1:r[1]], " ")
					}
					m["~line"] = fmt.Sprintf("%d", line)
					out <- m
				}
				break
			}
			if sigv != 0 {
				break
			}
		}
		if e := <-ierr; e != nil {
			panic(fmt.Errorf("problem reading TXT file %q (%v)", path, e))
		}
	}()
	return out, err, sig
}

// ReadCSV returns a channel into which a goroutine writes field maps of CSV lines from file at
// "path" keyed by "heads" map which also identifies select columns for extraction, or if nil, by
// the heads in the first data row (channels also provided for errors and for the caller to signal
// a halt).  CSV separator is "sep", or if \x00, will be inferred.  Fields are stripped of blanks
// and double-quotes (which may enclose separators); blank lines and those prefixed by "comment"
// are skipped.
func ReadCSV(path string, heads map[string]int, sep rune, comment string) (<-chan map[string]string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan map[string]string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer close(out)
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
		}()
		in, ierr, isig := readLn(path)
		defer close(isig)
		handleSig(sig, &sigv)

		vheads, vc, line, algn := make(map[string]int, 32), 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case sep == '\x00':
					max := 0
					for _, r := range ",\t|;:" {
						if c := len(splitCSV(ln, r)); c > max {
							max, sep = c, r
						}
					}
					continue
				case len(vheads) == 0:
					sln := splitCSV(ln, sep)
					for i, h := range sln {
						if h = strings.Trim(h, " "); h != "" && (len(heads) == 0 || heads[h] > 0) {
							vheads[h] = i + 1
						}
					}
					if vc = len(sln); len(heads) == 0 && len(vheads) < vc || len(vheads) < len(heads) {
						if len(heads) == 0 || len(vheads) > 0 {
							panic(fmt.Errorf("mismatched heads or missing header in CSV file %q", path))
						}
						cc, mc := make(map[int]int), 0
						for _, i := range heads {
							if i > 0 {
								cc[i]++
								if i > mc {
									mc = i
								}
							}
						}
						if mc > vc || len(cc) < len(heads) {
							panic(fmt.Errorf("bad head map provided for CSV file %q", path))
						}
						vheads = heads
						continue
					}
				default:
					if sln := splitCSV(ln, sep); len(sln) == vc {
						m, heading := make(map[string]string, len(vheads)), true
						for h, i := range vheads {
							f := strings.Trim(sln[i-1], " ")
							if heading && f != h {
								heading = false
							}
							m[h] = f
						}
						if !heading {
							m["~line"] = fmt.Sprintf("%d", line)
							out <- m
						}
					} else if algn++; line > 200 && float64(algn)/float64(line) > 0.05 {
						panic(fmt.Errorf("excessive column misalignment in CSV file %q (>%d rows)", path, algn))
					}
				}
				break
			}
			if sigv != 0 {
				break
			}
		}
		if e := <-ierr; e != nil {
			panic(fmt.Errorf("problem reading CSV file %q (%v)", path, e))
		}
	}()
	return out, err, sig
}
