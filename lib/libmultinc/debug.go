package libmultinc

import "log"

// debugfCat prints debug messages if the given category is enabled.
func debugfCat(cat string, format string, args ...interface{}) {
    if debugCategories[cat] {
        log.Printf(format, args...)
    }
}
