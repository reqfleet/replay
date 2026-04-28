package engine

import (
	"strings"
)

type PathTemplate struct {
	Original  string
	Parts     []string
	IsDynamic []bool
}

func ParsePathTemplates(templates []string) map[int][]PathTemplate {
	parsed := make(map[int][]PathTemplate)
	for _, tpl := range templates {
		if tpl == "" {
			continue
		}
		parts := strings.Split(strings.Trim(tpl, "/"), "/")
		isDynamic := make([]bool, len(parts))
		for i, part := range parts {
			if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
				isDynamic[i] = true
			}
		}

		length := len(parts)
		parsed[length] = append(parsed[length], PathTemplate{
			Original:  tpl,
			Parts:     parts,
			IsDynamic: isDynamic,
		})
	}
	return parsed
}

// MatchPathTemplate attempts to match a raw path against a map of pre-parsed parameterized templates
// grouped by segment length (e.g., "/users/{id}") and returns the matching template if found,
// otherwise returns the original.
func MatchPathTemplate(path string, templates map[int][]PathTemplate) string {
	if len(templates) == 0 {
		return path
	}

	trimmed := strings.Trim(path, "/")
	var length int
	if trimmed != "" {
		length = strings.Count(trimmed, "/") + 1
	} else {
		length = 1
	}
	tpls, ok := templates[length]
	if !ok {
		return path
	}

	if len(tpls) == 1 {
		// Fast path for single template: parse inline
		tpl := tpls[0]
		match := true
		s := trimmed
		for i := 0; i < length; i++ {
			var part string
			idx := strings.IndexByte(s, '/')
			if idx >= 0 {
				part = s[:idx]
				s = s[idx+1:]
			} else {
				part = s
			}

			if tpl.IsDynamic[i] {
				continue // dynamic part matches anything
			}
			if part != tpl.Parts[i] {
				match = false
				break
			}
		}
		if match {
			return tpl.Original
		}
		return path
	}

	// Multiple templates: parse into array once to avoid rescanning the string
	// Pre-allocate up to 16 parts on the stack to prevent heap allocations for most APIs
	var stackParts [16]string
	var parts []string
	if length <= 16 {
		parts = stackParts[:length]
	} else {
		parts = make([]string, length)
	}

	s := trimmed
	for i := 0; i < length; i++ {
		idx := strings.IndexByte(s, '/')
		if idx >= 0 {
			parts[i] = s[:idx]
			s = s[idx+1:]
		} else {
			parts[i] = s
		}
	}

	for _, tpl := range tpls {
		match := true
		for i := 0; i < length; i++ {
			if tpl.IsDynamic[i] {
				continue // dynamic part matches anything
			}
			if parts[i] != tpl.Parts[i] {
				match = false
				break
			}
		}

		if match {
			return tpl.Original
		}
	}

	return path
}
