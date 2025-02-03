package libmultinc

import (
    "fmt"
    "strconv"
    "strings"
)

func parsePortRange(s string) ([]int, error) {
    s = strings.TrimSpace(s)
    parts := strings.Split(s, "-")
    if len(parts) == 1 {
        p, err := strconv.Atoi(parts[0])
        if err != nil {
            return nil, fmt.Errorf("invalid port: %v", err)
        }
        return []int{p}, nil
    } else if len(parts) == 2 {
        start, err1 := strconv.Atoi(parts[0])
        end, err2 := strconv.Atoi(parts[1])
        if err1 != nil || err2 != nil {
            return nil, fmt.Errorf("invalid port range syntax: %q", s)
        }
        if end < start {
            return nil, fmt.Errorf("invalid range: %d-%d", start, end)
        }
        var ports []int
        for p := start; p <= end; p++ {
            ports = append(ports, p)
        }
        return ports, nil
    }
    return nil, fmt.Errorf("invalid port range: %q", s)
}
