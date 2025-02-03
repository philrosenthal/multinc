package libmultinc

func allocateMemory(size int64) {
    memoryMu.Lock()
    defer memoryMu.Unlock()

    for memoryUsed+size > memoryLimit {
        debugfCat("memory", "[memory] allocateMemory waiting: want=%d used=%d limit=%d",
            size, memoryUsed, memoryLimit)
        memoryCond.Wait()
    }
    memoryUsed += size
    debugfCat("memory", "[memory] allocateMemory: +%d => used=%d", size, memoryUsed)
}

func freeMemory(size int64) {
    memoryMu.Lock()
    memoryUsed -= size
    debugfCat("memory", "[memory] freeMemory: -%d => used=%d", size, memoryUsed)
    memoryMu.Unlock()
    memoryCond.Broadcast()
}
