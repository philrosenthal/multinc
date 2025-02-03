package libmultinc

import (
    "encoding/binary"
    "hash/crc32"
    "io"
)

const (
    msgTypeData   = 0
    msgTypeAck    = 1
    msgTypeResend = 2
    msgTypeEnd    = 3
)

type Block struct {
    Number   uint64
    Data     []byte
    Checksum uint32
}

func headerLength() int {
    // 1 byte msgType
    // 8 bytes blockNum
    // 4 bytes size
    // 4 bytes csum
    return 1 + 8 + 4 + 4
}

func encodeMessage(w io.Writer, msgType byte, blockNum uint64, data []byte, csum uint32) error {
    hdr := make([]byte, headerLength())
    hdr[0] = msgType
    binary.BigEndian.PutUint64(hdr[1:], blockNum)
    size := uint32(len(data))
    binary.BigEndian.PutUint32(hdr[1+8:], size)
    binary.BigEndian.PutUint32(hdr[1+8+4:], csum)

    if _, err := w.Write(hdr); err != nil {
        return err
    }
    if msgType == msgTypeData && size > 0 {
        if _, err := w.Write(data); err != nil {
            return err
        }
    }
    return nil
}

func decodeMessage(r io.Reader) (msgType byte, blockNum uint64, data []byte, csum uint32, err error) {
    hdr := make([]byte, headerLength())
    if _, err = io.ReadFull(r, hdr); err != nil {
        return 0, 0, nil, 0, err
    }
    msgType = hdr[0]
    blockNum = binary.BigEndian.Uint64(hdr[1:])
    size := binary.BigEndian.Uint32(hdr[1+8:])
    csum = binary.BigEndian.Uint32(hdr[1+8+4:])

    if msgType == msgTypeData && size > 0 {
        allocateMemory(int64(size))
        tmp := make([]byte, size)
        if _, err = io.ReadFull(r, tmp); err != nil {
            // free on partial read
            freeMemory(int64(size))
            return 0, 0, nil, 0, err
        }
        data = tmp
    }
    return
}

func computeChecksum(data []byte) uint32 {
    if !gConfig.EnableCsum {
        return 0
    }
    return crc32.ChecksumIEEE(data)
}
