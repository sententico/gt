package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Digest structure summarizing a CSV/TXT file for identification
type Digest struct {
	preview []string // preview rows (excluding non-blank non-comment lines)
	erows   int      // estimated total file rows
	comment string   // inferred comment line prefix
	sep     rune     // inferred field separator rune (if a CSV)
	split   []string // trimmed fields of first preview row split by "sep" (if a CSV)
}

const previewRows = 6
const sepSet = ",\t|;:"
const maxFieldLen = 256

var commentSet = [...]string{"#", "//", "'"}

// handleSig is a goroutine that monitors the "sig" channel; when closed, "sigv" is modified
func handleSig(sig <-chan int, sigv *int) {
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
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
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

// splitCSV returns a slice of fields in "csv" split by "sep", approximately following RFC 4180
func splitCSV(csv string, sep rune) (fields []string) {
	field, encl := "", false
	for _, r := range csv {
		switch {
		case r > '\x7e' || r != '\x09' && r < '\x20':
			// alternatively replace non-printables with a blank:field += " "
		case r == '"':
			encl = !encl
		case !encl && r == sep:
			fields = append(fields, field)
			field = ""
		default:
			field += string(r)
		}
	}
	return append(fields, field)
}

// PeekCSV returns a digest to identify the CSV (or TXT file) at "path". This digest consists of a
// preview slice of raw data rows (without blank or comment lines), a total file row estimate, the
// comment prefix used (if any), and if a CSV, the field separator with trimmed fields of the first
// data row split by it.
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
	info, e := file.Stat()
	if e != nil {
		panic(fmt.Errorf("can't access %q metadata (%v)", path, e))
	}
	bf, row, tlen, max := bufio.NewScanner(file), -1, 0, 1
getLine:
	for row < previewRows && bf.Scan() {
		switch ln := bf.Text(); {
		case len(strings.TrimLeft(ln, " ")) == 0:
		case dig.comment != "" && strings.HasPrefix(ln, dig.comment):
		case row < 0:
			row = 0
			for _, p := range commentSet {
				if strings.HasPrefix(ln, p) {
					dig.comment = p
					continue getLine
				}
			}
			fallthrough
		default:
			row++
			tlen += len(ln)
			dig.preview = append(dig.preview, ln)
		}
	}
	switch e := bf.Err(); {
	case e != nil:
		panic(fmt.Errorf("problem reading %q (%v)", path, e))
	case row < 1:
		panic(fmt.Errorf("%q does not contain data", path))
	case row < previewRows:
		dig.erows = row
	default:
		dig.erows = int(float64(info.Size())/float64(tlen-len(dig.preview[0])+row-1)*0.99+0.5) * (row - 1)
	}
getSep:
	for _, r := range sepSet {
		c, sl := 0, []string{}
		for _, ln := range dig.preview {
			if sl = splitCSV(ln, r); len(sl) <= max || len(sl) != c && c > 0 {
				continue getSep
			}
			for _, f := range sl {
				if len(f) > maxFieldLen {
					continue getSep
				}
			}
			c = len(sl)
		}
		max, dig.sep = c, r
	}
	if dig.sep > '\x00' {
		for _, f := range splitCSV(dig.preview[0], dig.sep) {
			dig.split = append(dig.split, strings.Trim(f, " "))
		}
	}
	return
}

// ReadTXT returns a channel into which a goroutine writes maps of fixed-field TXT rows from file
// at "path" keyed by "cols" (channels also provided for errors and for the caller to signal a
// halt).  Fields selected by byte ranges in the "cols" map are trimmed of blanks; empty fields
// are suppressed; blank lines and those prefixed by "comment" are skipped.
func ReadTXT(path string, cols map[string][2]int, comment string) (<-chan map[string]string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan map[string]string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
		}()
		in, ierr, isig := readLn(path)
		defer close(isig)
		handleSig(sig, &sigv)

		wid, line, algn := 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case wid == 0:
					wid = len(ln)
					if len(cols) == 0 || len(cols) > wid {
						panic(fmt.Errorf("missing or bad column map provided for TXT file %q", path))
					}
					for _, r := range cols { // TODO: could look for range overlaps
						if r[0] <= 0 || r[0] > r[1] || r[1] > wid {
							panic(fmt.Errorf("bad range in column map provided for TXT file %q", path))
						}
					}
					continue
				case len(ln) != wid:
					if algn++; line > 200 && float64(algn)/float64(line) > 0.05 {
						panic(fmt.Errorf("excessive column misalignment in TXT file %q (>%d rows)", path, algn))
					}
				default:
					m := make(map[string]string, len(cols))
					for c, r := range cols {
						if f := strings.Trim(ln[r[0]-1:r[1]], " "); len(f) > 0 {
							m[c] = f
						}
					}
					if len(m) > 0 {
						m["~line"] = fmt.Sprintf("%d", line)
						out <- m
					}
				}
				break
			}
			if sigv != 0 {
				return
			}
		}
		if e := <-ierr; e != nil {
			panic(fmt.Errorf("problem reading TXT file %q (%v)", path, e))
		}
	}()
	return out, err, sig
}

// ReadCSV returns a channel into which a goroutine writes field maps of CSV rows from file at
// "path" keyed by "cols" map which also identifies select columns for extraction, or if nil, by
// the heading in the first data row (channels also provided for errors and for the caller to
// signal a halt).  CSV separator is "sep", or if \x00, will be inferred.  Fields are trimmed of
// blanks and double-quotes (which may enclose separators); empty fields are suppressed; blank
// lines and those prefixed by "comment" are skipped.
func ReadCSV(path string, cols map[string]int, sep rune, comment string) (<-chan map[string]string, <-chan error, chan<- int) {
	out, err, sig, sigv := make(chan map[string]string, 64), make(chan error, 1), make(chan int), 0
	go func() {
		defer func() {
			if e := recover(); e != nil {
				err <- e.(error)
			}
			close(err)
			close(out)
		}()
		in, ierr, isig := readLn(path)
		defer close(isig)
		handleSig(sig, &sigv)

		vcols, wid, line, algn := make(map[string]int, 32), 0, 0, 0
		for ln := range in {
			for line++; ; {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case sep == '\x00':
					for _, r := range sepSet {
						if c := len(splitCSV(ln, r)); c > wid {
							wid, sep = c, r
						}
					}
					continue
				case len(vcols) == 0:
					sl, uc, sc, mc := splitCSV(ln, sep), make(map[int]int, len(cols)), make(map[string]int, len(cols)), 0
					for c, i := range cols {
						if c = strings.Trim(c, " "); c != "" && i > 0 {
							sc[c] = i
							if uc[i]++; i > mc {
								mc = i
							}
						}
					}
					for i, c := range sl {
						if c = strings.Trim(c, " "); c != "" && (len(sc) == 0 || sc[c] > 0) {
							vcols[c] = i + 1
						}
					}
					switch wid = len(sl); {
					case len(vcols) == wid:
					case len(sc) == 0:
						panic(fmt.Errorf("no heading in CSV file %q and no column map provided", path))
					case len(vcols) == len(sc):
					case len(vcols) > 0:
						panic(fmt.Errorf("missing columns in CSV file %q", path))
					case mc > wid:
						panic(fmt.Errorf("column map incompatible with CSV file %q", path))
					case len(uc) < len(sc):
						panic(fmt.Errorf("ambiguous column map provided for CSV file %q", path))
					default: // assume no heading; can't eliminate case where file is incompatible with sc
						vcols = sc
						continue
					}
				default:
					if sl := splitCSV(ln, sep); len(sl) == wid {
						m, heading := make(map[string]string, len(vcols)), true
						for c, i := range vcols {
							f := strings.Trim(sl[i-1], " ")
							if len(f) > 0 {
								m[c] = f
							}
							heading = heading && f == c
						}
						if !heading && len(m) > 0 {
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
				return
			}
		}
		if e := <-ierr; e != nil {
			panic(fmt.Errorf("problem reading CSV file %q (%v)", path, e))
		}
	}()
	return out, err, sig
}
