package redisadmin

import (
	"fmt"
	"strconv"
	"strings"
)

// Version is a server version as reported by INFO (Redis or Valkey).
type Version struct{ Major, Minor, Patch int }

// ParseVersion parses "8.2.1"; missing parts are 0 and any suffix such as
// "-valkey" is ignored.
func ParseVersion(s string) (Version, error) {
	core := s
	if i := strings.IndexFunc(s, func(r rune) bool { return r != '.' && (r < '0' || r > '9') }); i >= 0 {
		core = s[:i]
	}
	var parts [3]int
	for i, p := range strings.SplitN(core, ".", 3) {
		n, err := strconv.Atoi(p)
		if err != nil {
			return Version{}, fmt.Errorf("invalid version %q", s)
		}
		parts[i] = n
	}
	return Version{parts[0], parts[1], parts[2]}, nil
}

func (v Version) AtLeast(major, minor, patch int) bool {
	if v.Major != major {
		return v.Major > major
	}
	if v.Minor != minor {
		return v.Minor > minor
	}
	return v.Patch >= patch
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}
