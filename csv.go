package main

import (
	"bufio"
	"os"
	"strings"
)

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
		case r < 0x20 || r > 0x7e:
		default:
			field += string(r)
		}
	}
	return append(fields, field)
}

func handleSig(sig <-chan int, sigv *int) {
	*sigv = 0
	go func() {
		for *sigv = range sig {
		}
		*sigv = -1
	}()
}

// func sigClose(sig chan<- int, sigv *int) {
// 	if *sigv == 0 {
// 		close(sig)
// 		*sigv = -1
// 	} else if *sigv > 0 {
// 		sig <- *sigv
// 		*sigv = 0
// 	}
// }

func readLn(path string) (<-chan string, chan<- int) {
	out, sig, sigv := make(chan string, 64), make(chan int), 0
	go func() {
		defer close(out)
		file, err := os.Open(path)
		if err != nil {
			return
		}
		defer file.Close()
		handleSig(sig, &sigv)

		for ln := bufio.NewScanner(file); sigv == 0 && ln.Scan(); out <- ln.Text() {
		}
	}()
	return out, sig
}

// PeekCSV returns...(replaces PeekTXT)
func PeekCSV(path string) ([]string, string, rune, []string) {
	file, err := os.Open(path)
	if err != nil {
		return nil, "", 0, nil
	}
	defer file.Close()
	var peek []string
	comment, lnc, sep, max := "", -1, '\x00', 1

scan:
	for bf := bufio.NewScanner(file); lnc < 4 && bf.Scan(); {
		switch ln := bf.Text(); {
		case len(strings.TrimLeft(ln, " ")) == 0:
		case comment != "" && strings.HasPrefix(ln, comment):
		case lnc < 0:
			lnc = 0
			for _, p := range []string{"#", "//", "'"} {
				if strings.HasPrefix(ln, p) {
					comment = p
					continue scan
				}
			}
			fallthrough
		default:
			peek = append(peek, ln)
			lnc++
		}
	}
	if lnc <= 0 {
		return peek, comment, sep, nil
	}

	for _, r := range ",\t|;:" {
		p, c := 0, 0
		for _, ln := range peek {
			if c = len(splitCSV(ln, r)); c <= max || c != p && p > 0 {
				break
			}
			p = c
		}
		if c > max && c == p {
			max, sep = c, r
		}
	}
	if sep == '\x00' {
		return peek, comment, sep, nil
	}
	return peek, comment, sep, splitCSV(peek[0], sep)
}

// ReadTXT returns a channel into which a goroutine writes lines of text from file at "path".
func ReadTXT(path string, heads map[string][2]int, comment string) (<-chan map[string]string, chan<- int) {
	out, sig, sigv := make(chan map[string]string, 64), make(chan int), 0
	go func() {
		defer close(out)
		in, isig := readLn(path)
		defer close(isig)
		handleSig(sig, &sigv)

		vl := 0
		for ln := range in {
			for {
				switch {
				case len(strings.TrimLeft(ln, " ")) == 0:
				case comment != "" && strings.HasPrefix(ln, comment):
				case vl == 0:
					vl = len(ln)
					if len(heads) == 0 || len(heads) > vl {
						return
					}
					for _, r := range heads {
						if r[0] <= 0 || r[0] > r[1] || r[1] > vl {
							return
						}
					}
					continue
				case len(ln) != vl:
				default:
					m := make(map[string]string, len(heads))
					for h, r := range heads {
						m[h] = strings.Trim(ln[r[0]-1:r[1]], " ")
					}
					out <- m
				}
				break
			}
			if sigv != 0 {
				break
			}
		}
	}()
	return out, sig
}

// ReadCSV returns a channel into which a goroutine writes maps of CSV lines from file at "path"
// keyed by "heads" or if nil, by the heads in the first non-blank non-comment row.  CSV separator
// is "sep" unless 0x00; in which case, the separator will be inferred.  CSV heads and values are
// stripped of blanks and double-quotes.  If a prefix is specified in "comment", lines beginning
// with this prefix will be skipped.
func ReadCSV(path string, heads map[string]int, sep rune, comment string) (<-chan map[string]string, chan<- int) {
	out, sig, sigv := make(chan map[string]string, 64), make(chan int), 0
	go func() {
		defer close(out)
		in, isig := readLn(path)
		defer close(isig)
		handleSig(sig, &sigv)

		vheads, vc := make(map[string]int, 32), 0
		for ln := range in {
			for {
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
							return
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
							return
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
							out <- m
						}
					}
				}
				break
			}
			if sigv != 0 {
				break
			}
		}
	}()
	return out, sig
}
