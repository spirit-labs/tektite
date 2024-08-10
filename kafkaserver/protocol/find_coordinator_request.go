// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "unsafe"

type FindCoordinatorRequest struct {
    // The coordinator key.
    Key *string
    // The coordinator key type. (Group, transaction, etc.)
    KeyType int8
    // The coordinator keys.
    CoordinatorKeys []*string
}

func (m *FindCoordinatorRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version <= 3 {
        {
            // reading m.Key: The coordinator key.
            if version >= 3 {
                // flexible and not nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l0 := int(u - 1)
                s := string(buff[offset: offset + l0])
                m.Key = &s
                offset += l0
            } else {
                // non flexible and non nullable
                var l0 int
                l0 = int(binary.BigEndian.Uint16(buff[offset:]))
                offset += 2
                s := string(buff[offset: offset + l0])
                m.Key = &s
                offset += l0
            }
        }
    }
    if version >= 1 {
        {
            // reading m.KeyType: The coordinator key type. (Group, transaction, etc.)
            m.KeyType = int8(buff[offset])
            offset++
        }
    }
    if version >= 4 {
        {
            // reading m.CoordinatorKeys: The coordinator keys.
            var l1 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 = int(u - 1)
            if l1 >= 0 {
                // length will be -1 if field is null
                coordinatorKeys := make([]*string, l1)
                for i0 := 0; i0 < l1; i0++ {
                    // flexible and not nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l2 := int(u - 1)
                    s := string(buff[offset: offset + l2])
                    coordinatorKeys[i0] = &s
                    offset += l2
                }
                m.CoordinatorKeys = coordinatorKeys
            }
        }
    }
    if version >= 3 {
        // reading tagged fields
        nt, n := binary.Uvarint(buff[offset:])
        offset += n
        for i := 0; i < int(nt); i++ {
            t, n := binary.Uvarint(buff[offset:])
            offset += n
            ts, n := binary.Uvarint(buff[offset:])
            offset += n
            switch t {
                default:
                    offset += int(ts)
            }
        }
    }
    return offset, nil
}

func (m *FindCoordinatorRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version <= 3 {
        // writing m.Key: The coordinator key.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*m.Key) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.Key)))
        }
        if m.Key != nil {
            buff = append(buff, *m.Key...)
        }
    }
    if version >= 1 {
        // writing m.KeyType: The coordinator key type. (Group, transaction, etc.)
        buff = append(buff, byte(m.KeyType))
    }
    if version >= 4 {
        // writing m.CoordinatorKeys: The coordinator keys.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.CoordinatorKeys) + 1))
        for _, coordinatorKeys := range m.CoordinatorKeys {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*coordinatorKeys) + 1))
            if coordinatorKeys != nil {
                buff = append(buff, *coordinatorKeys...)
            }
        }
    }
    if version >= 3 {
        numTaggedFields3 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields3))
    }
    return buff
}

func (m *FindCoordinatorRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version <= 3 {
        // size for m.Key: The coordinator key.
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(*m.Key) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if m.Key != nil {
            size += len(*m.Key)
        }
    }
    if version >= 1 {
        // size for m.KeyType: The coordinator key type. (Group, transaction, etc.)
        size += 1
    }
    if version >= 4 {
        // size for m.CoordinatorKeys: The coordinator keys.
        // flexible and not nullable
        size += sizeofUvarint(len(m.CoordinatorKeys) + 1)
        for _, coordinatorKeys := range m.CoordinatorKeys {
            size += 0 * int(unsafe.Sizeof(coordinatorKeys)) // hack to make sure loop variable is always used
            // flexible and not nullable
            size += sizeofUvarint(len(*coordinatorKeys) + 1)
            if coordinatorKeys != nil {
                size += len(*coordinatorKeys)
            }
        }
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 3 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *FindCoordinatorRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 3 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *FindCoordinatorRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
