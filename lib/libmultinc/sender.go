package libmultinc

import (
    "bufio"
    "io"
    "math/rand"
    "net"
    "sync"
    "time"
)

// senderState tracks in-flight blocks on the sender.
type senderState struct {
    mu              sync.Mutex
    cond            *sync.Cond
    nextBlockNumber uint64
    inFlight        map[uint64]*Block
    doneSending     bool
}

func newSenderState() *senderState {
    ss := &senderState{
        inFlight: make(map[uint64]*Block),
    }
    ss.cond = sync.NewCond(&ss.mu)
    return ss
}

func (ss *senderState) addBlock(data []byte) *Block {
    allocateMemory(int64(len(data)))

    ss.mu.Lock()
    defer ss.mu.Unlock()

    bn := ss.nextBlockNumber
    ss.nextBlockNumber++
    csum := computeChecksum(data)
    bl := &Block{Number: bn, Data: data, Checksum: csum}
    ss.inFlight[bn] = bl

    debugfCat("send", "[senderState] addBlock: blockNum=%d (inFlight=%d)", bn, len(ss.inFlight))
    return bl
}

func (ss *senderState) markAckCumulative(blockNum uint64) {
    ss.mu.Lock()
    defer ss.mu.Unlock()

    before := len(ss.inFlight)
    for bn, bl := range ss.inFlight {
        if bn <= blockNum {
            freeMemory(int64(len(bl.Data)))
            delete(ss.inFlight, bn)
        }
    }
    after := len(ss.inFlight)
    debugfCat("ack", "[senderState] markAckCumulative(%d): inFlight %d --> %d", blockNum, before, after)
    ss.cond.Broadcast()
}

func (ss *senderState) getBlock(blockNum uint64) *Block {
    ss.mu.Lock()
    defer ss.mu.Unlock()
    return ss.inFlight[blockNum]
}

func (ss *senderState) markDone() {
    ss.mu.Lock()
    ss.doneSending = true
    ss.mu.Unlock()
    ss.cond.Broadcast()
    debugfCat("send", "[senderState] markDone: no more new blocks")
}

func (ss *senderState) allAcked() bool {
    ss.mu.Lock()
    defer ss.mu.Unlock()
    return ss.doneSending && len(ss.inFlight) == 0
}

// dataSender handles the actual TCP data sockets to the receiver.
type dataSender struct {
    address    string
    blockChan  chan *Block
    closeCh    chan struct{}
    wg         sync.WaitGroup
    retryCount int
}

func newDataSender(address string) *dataSender {
    return &dataSender{
        address:   address,
        blockChan: make(chan *Block, 1024),
        closeCh:   make(chan struct{}),
    }
}

func (ds *dataSender) start(retryLimit int) {
    ds.wg.Add(1)
    go ds.run(retryLimit)
}

func (ds *dataSender) run(retryLimit int) {
    defer ds.wg.Done()

    for {
        debugfCat("conn", "[dataSender %s] attempting dial", ds.address)
        conn, err := net.Dial("tcp", ds.address)
        if err != nil {
            ds.retryCount++
            debugfCat("conn", "[dataSender %s] dial error: %v", ds.address, err)
            if retryLimit > 0 && ds.retryCount >= retryLimit {
                debugfCat("conn", "[dataSender %s] giving up after %d retries", ds.address, ds.retryCount)
                return
            }
            select {
            case <-time.After(gConfig.RetryDelay):
                continue
            case <-ds.closeCh:
                return
            }
        }
        ds.retryCount = 0
        debugfCat("conn", "[dataSender %s] connected", ds.address)

        sendErrCh := make(chan error, 1)
        go func(c net.Conn) {
            defer c.Close()
            for {
                select {
                case bl := <-ds.blockChan:
                    if bl == nil {
                        debugfCat("conn", "[dataSender %s] got sentinel nil, closing", ds.address)
                        return
                    }
                    debugfCat("send", "[dataSender %s] sending blockNum=%d size=%d", ds.address, bl.Number, len(bl.Data))
                    if err := encodeMessage(c, msgTypeData, bl.Number, bl.Data, bl.Checksum); err != nil {
                        sendErrCh <- err
                        return
                    }
                case <-ds.closeCh:
                    debugfCat("conn", "[dataSender %s] closeCh triggered", ds.address)
                    return
                }
            }
        }(conn)

        // Wait for either a send error or a close signal
        select {
        case sendErr := <-sendErrCh:
            conn.Close()
            debugfCat("conn", "[dataSender %s] send error: %v", ds.address, sendErr)
            select {
            case <-ds.closeCh:
                return
            default:
                continue
            }
        case <-ds.closeCh:
            conn.Close()
            return
        }
    }
}

func (ds *dataSender) enqueueBlock(bl *Block) {
    ds.blockChan <- bl
}

func (ds *dataSender) close() {
    close(ds.closeCh)
    ds.blockChan <- nil // sentinel
    ds.wg.Wait()
}

// senderControlLoop handles incoming ack/resend from the receiver on the control channel.
func senderControlLoop(conn net.Conn, ss *senderState, dataSenders []*dataSender, fallbackCh chan *Block) {
    defer conn.Close()
    dec := bufio.NewReader(conn)
    for {
        msgType, blockNum, _, _, err := decodeMessage(dec)
        if err != nil {
            if err == io.EOF {
                debugfCat("conn", "[sender-control] EOF on control connection")
            } else {
                debugfCat("conn", "[sender-control] decode error: %v", err)
            }
            return
        }
        debugfCat("conn", "[sender-control] msgType=%d blockNum=%d", msgType, blockNum)
        switch msgType {
        case msgTypeAck:
            ss.markAckCumulative(blockNum)
        case msgTypeResend:
            bl := ss.getBlock(blockNum)
            if bl != nil {
                debugfCat("resend", "[sender-control] Resending blockNum=%d", blockNum)
                dispatchBlock(bl, dataSenders, fallbackCh)
            } else {
                debugfCat("resend", "[sender-control] blockNum=%d not found in-flight", blockNum)
            }
        case msgTypeEnd:
            // no-op, just ignore it
            debugfCat("conn", "[sender-control] got msgTypeEnd; ignoring")
        case msgTypeData:
            // not expected from receiver
        }
    }
}

// dispatchBlock sends a block to a random dataSender or fallback if none exist.
func dispatchBlock(bl *Block, dataSenders []*dataSender, fallbackCh chan *Block) {
    if len(dataSenders) == 0 {
        // single-socket fallback
        fallbackCh <- bl
    } else {
        i := rand.Intn(len(dataSenders))
        dataSenders[i].enqueueBlock(bl)
    }
}
