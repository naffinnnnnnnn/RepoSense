package repository

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

var windowsDrivePath = regexp.MustCompile(`^[A-Za-z]:($|/)`)

// CanonicalPath converts a repository path to its single, slash-separated form.
// Repository paths are always relative and may never escape the repository root.
func CanonicalPath(value string) (string, error) {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, "/"))
	if value == "" || strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") || windowsDrivePath.MatchString(value) {
		return "", fmt.Errorf("路径必须是仓库相对路径")
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == ".." {
			return "", fmt.Errorf("路径不能包含父目录穿越")
		}
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("路径必须指向仓库内文件")
	}
	return clean, nil
}

// CanonicalPattern applies repository-path safety rules while preserving glob
// metacharacters. A double star matches zero or more complete path segments.
func CanonicalPattern(value string) (string, error) {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, "/"))
	if value == "" || strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") || windowsDrivePath.MatchString(value) {
		return "", fmt.Errorf("包含路径无效：%q", value)
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == ".." {
			return "", fmt.Errorf("包含路径无效：%q", value)
		}
	}
	clean := path.Clean(value)
	if _, err := globRegexp(clean); err != nil {
		return "", fmt.Errorf("包含路径无效：%q", value)
	}
	return clean, nil
}

func MatchPattern(pattern, file string) (bool, error) {
	pattern, err := CanonicalPattern(pattern)
	if err != nil {
		return false, err
	}
	file, err = CanonicalPath(file)
	if err != nil {
		return false, err
	}
	re, err := globRegexp(pattern)
	if err != nil {
		return false, err
	}
	return re.MatchString(file), nil
}

func globRegexp(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); {
		switch pattern[i] {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				i += 2
				if i < len(pattern) && pattern[i] == '/' {
					i++
					b.WriteString("(?:.*/)?")
				} else {
					b.WriteString(".*")
				}
			} else {
				i++
				b.WriteString("[^/]*")
			}
		case '?':
			i++
			b.WriteString("[^/]")
		case '[':
			end := strings.IndexByte(pattern[i+1:], ']')
			if end < 0 {
				return nil, fmt.Errorf("未闭合的字符集合")
			}
			end += i + 1
			b.WriteString(pattern[i : end+1])
			i = end + 1
		default:
			b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
			i++
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}
