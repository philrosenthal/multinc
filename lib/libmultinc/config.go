package libmultinc

import (
    "math/rand"
    "strings"
    "sync"
    "time"
)

// Config holds what used to be the flag-based configuration.
type Config struct {
    ListenPorts         string
    Dest               string
    BlockSize          int
    EnableCsum         bool
    RetryDelay         time.Duration
    RetryLimit         int
    OutOfOrderThreshold int
    MissingBlockTimeout time.Duration
    PartialBlockTimeout time.Duration
    DebugCategories    string
    MemoryLimit        int64
}

// Global package variables for config and debug.
var (
    gConfig         Config
    debugCategories map[string]bool

    // Memory-limit variables:
    memoryLimit int64
    memoryUsed  int64
    memoryMu    sync.Mutex
    memoryCond  = sync.NewCond(&memoryMu)
)

// InitializeConfig sets our global gConfig, sets up debug categories, seeds RNG, etc.
func InitializeConfig(c *Config) {
    gConfig = *c
    parseDebugCategories(gConfig.DebugCategories)

    // Default 1GB memory limit if not set
    memoryLimit = gConfig.MemoryLimit
    if memoryLimit == 0 {
        memoryLimit = 1024 * 1024 * 1024
    }

    rand.Seed(time.Now().UnixNano())
}

func parseDebugCategories(catString string) {
    debugCategories = make(map[string]bool)
    if catString == "" {
        return
    }
    cats := strings.Split(catString, ",")
    for _, c := range cats {
        c = strings.TrimSpace(c)
        if c != "" {
            debugCategories[c] = true
        }
    }
}
