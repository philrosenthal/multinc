package libmultinc

import (
    "bufio"
    "errors"
    "fmt"
    "io"
    "log"
    "net"
    "sync"
    "time"
)

type receiverState struct {
    mu              sync.Mutex
    doneCond        *sync.Cond
    nextBlockNeeded uint64
    outOfOrder      map[uint64]*Block
    doneReceiving   bool

    missingSince map[uint64]time.Time
    writer       io.Writer

    recentRequests      []time.Time
    lastBulkRequestTime time.Time
    bulkRequestedBlocks map[uint64]bool
}

func newReceiverState(w io.Writer) *receiverState {
    rs := &receiverState{
        nextBlockNeeded:     0,
        outOfOrder:          make(map[uint64]*Block),
        missingSince:        make(map[uint64]time.Time),
        writer:              w,
        recentRequests:      make([]time.Time, 0, 20),
        lastBulkRequestTime: time.Time{},
        bulkRequestedBlocks: make(map[uint64]bool),
    }
    rs.doneCond = sync.NewCond(&rs.mu)
    return rs
}

func (rs *receiverState) handleDataBlock(block *Block) (uint64, bool) {
    rs.mu.Lock()
    defer rs.mu.Unlock()

    localCsum := computeChecksum(block.Data)
    if localCsum != block.Checksum {
        debugfCat("recv", "[receiverState] BAD CHECKSUM for block #%d", block.Number)
        freeMemory(int64(len(block.Data)))
        return 0, true
    }

    // If block is stale or already present
    if block.Number < rs.nextBlockNeeded || rs.outOfOrder[block.Number] != nil {
        freeMemory(int64(len(block.Data)))
        return 0, false
    }

    rs.outOfOrder[block.Number] = block

    var highestAdvanced uint64
    // consume consecutive blocks
    for {
        bl2, ok := rs.outOfOrder[rs.nextBlockNeeded]
        if !ok {
            break
        }
        _, err := rs.writer.Write(bl2.Data)
        if err != nil {
            debugfCat("recv", "[receiverState] write error on block %d: %v", rs.nextBlockNeeded, err)
        }
        delete(rs.outOfOrder, rs.nextBlockNeeded)
        delete(rs.missingSince, rs.nextBlockNeeded)

        freeMemory(int64(len(bl2.Data)))

        highestAdvanced = rs.nextBlockNeeded
        rs.nextBlockNeeded++
    }

    if highestAdvanced > 0 {
        debugfCat("recv", "[receiverState] advanced nextBlockNeeded to %d", rs.nextBlockNeeded)
    }
    return highestAdvanced, false
}

func (rs *receiverState) handleEnd() {
    rs.mu.Lock()
    rs.doneReceiving = true
    rs.mu.Unlock()
    debugfCat("recv", "[receiverState] handleEnd: doneReceiving = true")
    rs.doneCond.Broadcast()
}

func (rs *receiverState) isComplete() bool {
    if !rs.doneReceiving {
        return false
    }
    return len(rs.outOfOrder) == 0
}

func (rs *receiverState) requestResendForBlock(needed uint64, controlOut chan<- uint64) {
    // If we already requested this block in a recent bulk request, skip
    if rs.bulkRequestedBlocks[needed] {
        debugfCat("resend", "[receiverState] skipping resend request for block #%d (in last bulk)", needed)
        return
    }
    now := time.Now()
    rs.recentRequests = append(rs.recentRequests, now)
    // prune old entries
    cutoff := now.Add(-1 * time.Minute)
    idx := 0
    for _, t := range rs.recentRequests {
        if t.After(cutoff) {
            rs.recentRequests[idx] = t
            idx++
        }
    }
    rs.recentRequests = rs.recentRequests[:idx]

    debugfCat("resend", "[receiverState] requesting resend for block #%d (single)", needed)
    select {
    case controlOut <- needed:
    default:
        debugfCat("resend", "[receiverState] controlOut channel full? skipping block #%d", needed)
    }
}

func (rs *receiverState) doBulkRequest(controlOut chan<- uint64) {
    debugfCat("resend", "[receiverState] performing BULK re-send request")
    rs.bulkRequestedBlocks = make(map[uint64]bool)
    var highestReceived uint64
    for k := range rs.outOfOrder {
        if k > highestReceived {
            highestReceived = k
        }
    }
    if highestReceived < rs.nextBlockNeeded {
        debugfCat("resend", "[receiverState] doBulkRequest: highest(%d) < nextNeeded(%d)? no action",
            highestReceived, rs.nextBlockNeeded)
        return
    }

    for bn := rs.nextBlockNeeded; bn <= highestReceived; bn++ {
        if _, ok := rs.outOfOrder[bn]; !ok {
            rs.bulkRequestedBlocks[bn] = true
        }
    }

    debugfCat("resend", "[receiverState] doBulkRequest: missing %d blocks", len(rs.bulkRequestedBlocks))
    for missingBn := range rs.bulkRequestedBlocks {
        select {
        case controlOut <- missingBn:
        default:
            debugfCat("resend", "[receiverState] doBulkRequest: controlOut full? skipping #%d", missingBn)
        }
    }
}

func (rs *receiverState) monitorMissingBlocks(controlOut chan<- uint64, threshold int, timeout time.Duration) {
    ticker := time.NewTicker(200 * time.Millisecond)
    defer ticker.Stop()

    for {
        <-ticker.C
        rs.mu.Lock()
        if rs.isComplete() {
            rs.mu.Unlock()
            return
        }

        needed := rs.nextBlockNeeded
        var highest uint64
        for k := range rs.outOfOrder {
            if k > highest {
                highest = k
            }
        }

        // single-block request if we exceed threshold
        if highest >= needed+uint64(threshold) {
            t, ok := rs.missingSince[needed]
            if !ok {
                rs.missingSince[needed] = time.Now()
            } else {
                if time.Since(t) >= timeout {
                    rs.requestResendForBlock(needed, controlOut)
                    rs.missingSince[needed] = time.Now()
                }
            }
        }

        // If >3 requests in last minute, do bulk
        if len(rs.recentRequests) > 3 {
            now := time.Now()
            if now.Sub(rs.lastBulkRequestTime) > time.Minute {
                rs.doBulkRequest(controlOut)
                rs.lastBulkRequestTime = now
            }
        }

        // Clear old bulkRequestedBlocks after a minute
        if time.Since(rs.lastBulkRequestTime) > time.Minute && len(rs.bulkRequestedBlocks) > 0 {
            debugfCat("resend", "[receiverState] clearing bulkRequestedBlocks after 1 minute")
            rs.bulkRequestedBlocks = make(map[uint64]bool)
        }

        rs.mu.Unlock()
    }
}

// dataReceiver listens on a data port and processes incoming blocks.
type dataReceiver struct {
    port   int
    rs     *receiverState
    ackCh  chan<- uint64
    wg     sync.WaitGroup
    closeC chan struct{}
}

func newDataReceiver(port int, rs *receiverState, ackCh chan<- uint64) *dataReceiver {
    return &dataReceiver{
        port:   port,
        rs:     rs,
        ackCh:  ackCh,
        closeC: make(chan struct{}),
    }
}

func (dr *dataReceiver) start() {
    dr.wg.Add(1)
    go dr.run()
}

func (dr *dataReceiver) run() {
    defer dr.wg.Done()
    ln, err := net.Listen("tcp", fmt.Sprintf(":%d", dr.port))
    if err != nil {
        log.Fatalf("[dataReceiver] cannot listen on port %d: %v", dr.port, err)
    }
    defer ln.Close()
    debugfCat("conn", "[dataReceiver %d] listening", dr.port)

    for {
        connCh := make(chan net.Conn, 1)
        errCh := make(chan error, 1)

        go func() {
            c, acceptErr := ln.Accept()
            if acceptErr != nil {
                errCh <- acceptErr
                return
            }
            connCh <- c
        }()

        select {
        case <-dr.closeC:
            debugfCat("conn", "[dataReceiver %d] closeC triggered", dr.port)
            return
        case acceptErr := <-errCh:
            debugfCat("conn", "[dataReceiver %d] accept error: %v", dr.port, acceptErr)
            time.Sleep(2 * time.Second)
            continue
        case c := <-connCh:
            debugfCat("conn", "[dataReceiver %d] accepted connection", dr.port)
            go dr.handleConn(c)
        }
    }
}

func (dr *dataReceiver) handleConn(c net.Conn) {
    defer c.Close()
    dec := bufio.NewReader(c)
    for {
        msgType, blockNum, data, csum, err := decodeMessage(dec)
        if err != nil {
            if errors.Is(err, io.EOF) {
                debugfCat("conn", "[dataReceiver %d] EOF on data connection", dr.port)
                return
            }
            debugfCat("conn", "[dataReceiver %d] decode error: %v", dr.port, err)
            return
        }
        if msgType != msgTypeData {
            debugfCat("conn", "[dataReceiver %d] ignoring msgType=%d blockNum=%d", dr.port, msgType, blockNum)
            continue
        }
        bl := &Block{Number: blockNum, Data: data, Checksum: csum}
        highestAdvanced, badCsum := dr.rs.handleDataBlock(bl)
        if badCsum {
            continue
        }
        if highestAdvanced > 0 {
            dr.ackCh <- highestAdvanced
        }
    }
}

func (dr *dataReceiver) close() {
    close(dr.closeC)
    dr.wg.Wait()
}

// receiverControlLoop reads control messages from the sender (like "End").
func receiverControlLoop(conn net.Conn, rs *receiverState, wg *sync.WaitGroup) {
    defer func() {
        debugfCat("conn", "[receiver-control] closing conn")
        conn.Close()
        wg.Done()
    }()
    dec := bufio.NewReader(conn)
    for {
        msgType, _, _, _, err := decodeMessage(dec)
        if err != nil {
            if errors.Is(err, io.EOF) {
                debugfCat("conn", "[receiver-control] EOF on control connection")
            } else {
                debugfCat("conn", "[receiver-control] decode error: %v", err)
            }
            return
        }
        if msgType == msgTypeEnd {
            rs.handleEnd()
        }
    }
}

// Control messages used internally by the receiver's aggregator
type controlMessage struct {
    msgType  byte
    blockNum uint64
}

// receiverControlWriterLoop sends ack/resend messages to the sender
func receiverControlWriterLoop(conn net.Conn, msgChan <-chan controlMessage, wg *sync.WaitGroup) {
    defer func() {
        debugfCat("conn", "[receiver-control-writer] closing conn")
        conn.Close()
        wg.Done()
    }()
    for cm := range msgChan {
        switch cm.msgType {
        case msgTypeAck:
            debugfCat("ack", "[receiver] sending ACK for blockNum=%d", cm.blockNum)
            _ = encodeMessage(conn, msgTypeAck, cm.blockNum, nil, 0)
        case msgTypeResend:
            debugfCat("resend", "[receiver] requesting resend for blockNum=%d", cm.blockNum)
            _ = encodeMessage(conn, msgTypeResend, cm.blockNum, nil, 0)
        }
    }
    debugfCat("conn", "[receiver-control-writer] msgChan closed, done.")
}
