package orbitflare

import "strings"

func ApplyAPIKey(url, apiKey string) string {
	if apiKey == "" || strings.Contains(url, "api_key=") {
		return url
	}
	var base, sep string
	switch {
	case strings.Contains(url, "?"):
		base, sep = url, "&"
	case hasPath(url):
		base, sep = url, "?"
	default:
		base, sep = url+"/", "?"
	}
	return base + sep + "api_key=" + apiKey
}

func hasPath(url string) bool {
	afterScheme := url
	if _, rest, ok := strings.Cut(url, "://"); ok {
		afterScheme = rest
	}
	return strings.Contains(afterScheme, "/")
}

func SanitizeURL(s string) string {
	result := s
	for _, needle := range []string{"?api_key=", "&api_key="} {
		for {
			start := strings.Index(result, needle)
			if start < 0 {
				break
			}
			valueStart := start + len(needle)
			end := len(result)
			if i := strings.IndexFunc(result[valueStart:], isValueTerminator); i >= 0 {
				end = i + valueStart
			}
			result = result[:start] + result[end:]
		}
	}
	return result
}

func isValueTerminator(r rune) bool {

	return r == '&' || r == ')' || r == '"' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
}
