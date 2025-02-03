package libmultinc

import (
    "fmt"
    "net"
    "bufio"
    "os"
    "sort"
    "strings"
    "sync"
    "time"
)

// RunReceiver is the main entry for receiver mode.
func RunReceiver(cfg *Config) error {
    InitializeConfig(cfg)

    // parse the port range
    ports, err := parsePortRange(gConfig.ListenPorts)
    if err != nil {
        return fmt.Errorf("invalid listen port range %q: %v", gConfig.ListenPorts, err)
    }
    sort.Ints(ports)
    if len(ports) == 0 {
        return fmt.Errorf("no ports specified in %q", gConfig.ListenPorts)
    }

    controlPort := ports[0]
    dataPorts := ports[1:]
    numDataSockets := len(dataPorts)

    // set default if needed
    if gConfig.OutOfOrderThreshold == 0 {
        if numDataSockets > 0 {
            gConfig.OutOfOrderThreshold = 4 * numDataSockets
        } else {
            gConfig.OutOfOrderThreshold = 4
        }
    }

    rs := newReceiverState(os.Stdout)
    controlMsgChan := make(chan controlMessage, 1000)
    ackChan := make(chan uint64, 1000)
    resendChan := make(chan uint64, 100)

    // missing-block monitor
    go rs.monitorMissingBlocks(resendChan, gConfig.OutOfOrderThreshold, gConfig.MissingBlockTimeout)

    // Data receivers
    var drs []*dataReceiver
    for _, dp := range dataPorts {
        dr := newDataReceiver(dp, rs, ackChan)
        drs = append(drs, dr)
        dr.start()
    }

    // Listen for control
    ln, err := net.Listen("tcp", fmt.Sprintf(":%d", controlPort))
    if err != nil {
        return fmt.Errorf("cannot listen on control port %d: %v", controlPort, err)
    }
    defer ln.Close()

    conn, err := ln.Accept()
    if err != nil {
        return fmt.Errorf("control accept error on port %d: %v", controlPort, err)
    }

    var controlWg sync.WaitGroup
    controlWg.Add(3) // read loop + writer loop + aggregator

    // (1) Control reading loop
    go receiverControlLoop(conn, rs, &controlWg)

    // (2) Single control writing loop
    go receiverControlWriterLoop(conn, controlMsgChan, &controlWg)

    // ACK aggregator => push ack -> controlMsgChan
    go func() {
        defer func() {
            controlWg.Done()
        }()
        for bn := range ackChan {
            controlMsgChan <- controlMessage{msgType: msgTypeAck, blockNum: bn}
        }
    }()

    // Resend aggregator => push resend -> controlMsgChan
    go func() {
        for bn := range resendChan {
            controlMsgChan <- controlMessage{msgType: msgTypeResend, blockNum: bn}
        }
    }()

    // Wait until the receiver is complete
    rs.mu.Lock()
    for !rs.isComplete() {
        rs.doneCond.Wait()
    }
    rs.mu.Unlock()

    // close data receivers
    for _, dr := range drs {
        dr.close()
    }

    close(ackChan)
    close(resendChan)
    close(controlMsgChan)
    controlWg.Wait()

    return nil
}

// RunSender is the main entry for sender mode.
func RunSender(cfg *Config) error {
    InitializeConfig(cfg)

    parts := strings.SplitN(gConfig.Dest, ":", 2)
    if len(parts) != 2 {
        return fmt.Errorf("invalid -dest format %q (must be host:port or host:start-end)", gConfig.Dest)
    }
    host := parts[0]
    portStr := parts[1]

    ports, err := parsePortRange(portStr)
    if err != nil {
        return fmt.Errorf("invalid dest port range %q: %v", portStr, err)
    }
    sort.Ints(ports)
    if len(ports) == 0 {
        return fmt.Errorf("no ports specified in Dest=%q", gConfig.Dest)
    }

    controlPort := ports[0]
    dataPorts := ports[1:]
    numDataSockets := len(dataPorts)

    if gConfig.OutOfOrderThreshold == 0 && numDataSockets > 0 {
        gConfig.OutOfOrderThreshold = 4 * numDataSockets
    }

    ss := newSenderState()

    // Connect control
    controlAddr := fmt.Sprintf("%s:%d", host, controlPort)
    var conn net.Conn
    for {
        c, err := net.Dial("tcp", controlAddr)
        if err != nil {
            debugfCat("conn", "[runSender] dial error to control %s: %v", controlAddr, err)
            time.Sleep(gConfig.RetryDelay)
            continue
        }
        conn = c
        break
    }

    // data senders
    var ds []*dataSender
    for _, dp := range dataPorts {
        addr := fmt.Sprintf("%s:%d", host, dp)
        d := newDataSender(addr)
        ds = append(ds, d)
        d.start(gConfig.RetryLimit)
    }

    // fallback single-socket if no data ports
    var fallbackCh chan *Block
    if len(dataPorts) == 0 {
        fallbackCh = make(chan *Block, 1000)
        go func() {
            for bl := range fallbackCh {
                if bl == nil {
                    debugfCat("conn", "[sender-fallback] got nil, exiting fallback loop")
                    return
                }
                _ = encodeMessage(conn, msgTypeData, bl.Number, bl.Data, bl.Checksum)
            }
        }()
    }

    // control loop
    go senderControlLoop(conn, ss, ds, fallbackCh)

    // read from stdin in a separate goroutine
    readChan := make(chan []byte, 20)
    go func() {
        bufIn := bufio.NewReader(os.Stdin)
        for {
            buf := make([]byte, 32*1024)
            n, err := bufIn.Read(buf)
            if n > 0 {
                chunk := make([]byte, n)
                copy(chunk, buf[:n])
                readChan <- chunk
            }
            if err != nil {
                close(readChan)
                return
            }
        }
    }()

    blockBuf := make([]byte, 0, gConfig.BlockSize)
    timer := time.NewTimer(gConfig.PartialBlockTimeout)

mainLoop:
    for {
        timer.Reset(gConfig.PartialBlockTimeout)
        select {
        case chunk, ok := <-readChan:
            if !ok {
                // flush last partial
                if len(blockBuf) > 0 {
                    bl := ss.addBlock(append([]byte(nil), blockBuf...))
                    dispatchBlock(bl, ds, fallbackCh)
                }
                break mainLoop
            }
            offset := 0
            for offset < len(chunk) {
                space := gConfig.BlockSize - len(blockBuf)
                toCopy := len(chunk) - offset
                if toCopy > space {
                    toCopy = space
                }
                blockBuf = append(blockBuf, chunk[offset:offset+toCopy]...)
                offset += toCopy

                if len(blockBuf) == gConfig.BlockSize {
                    bl := ss.addBlock(append([]byte(nil), blockBuf...))
                    dispatchBlock(bl, ds, fallbackCh)
                    blockBuf = blockBuf[:0]
                }
            }

        case <-timer.C:
            if len(blockBuf) > 0 {
                bl := ss.addBlock(append([]byte(nil), blockBuf...))
                dispatchBlock(bl, ds, fallbackCh)
                blockBuf = blockBuf[:0]
            }
        }
    }

    ss.markDone()

    // wait for all blocks acked
    for {
        time.Sleep(500 * time.Millisecond)
        if ss.allAcked() {
            // send msgTypeEnd to receiver
            _ = encodeMessage(conn, msgTypeEnd, 0, nil, 0)
            break
        }
    }

    if fallbackCh != nil {
        fallbackCh <- nil
        close(fallbackCh)
    }
    for _, d := range ds {
        d.close()
    }
    conn.Close()

    return nil
}
