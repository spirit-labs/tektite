// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type DescribeGroupsRequest struct {
    // The names of the groups to describe
    Groups []*string
    // Whether to include authorized operations.
    IncludeAuthorizedOperations bool
}

func (m *DescribeGroupsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Groups: The names of the groups to describe
        var l0 int
        if version >= 5 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 = int(u - 1)
        } else {
            // non flexible and non nullable
            l0 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        if l0 >= 0 {
            // length will be -1 if field is null
            groups := make([]*string, l0)
            for i0 := 0; i0 < l0; i0++ {
                if version >= 5 {
                    // flexible and not nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l1 := int(u - 1)
                    s := string(buff[offset: offset + l1])
                    groups[i0] = &s
                    offset += l1
                } else {
                    // non flexible and non nullable
                    var l1 int
                    l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                    s := string(buff[offset: offset + l1])
                    groups[i0] = &s
                    offset += l1
                }
            }
            m.Groups = groups
        }
    }
    if version >= 3 {
        {
            // reading m.IncludeAuthorizedOperations: Whether to include authorized operations.
            m.IncludeAuthorizedOperations = buff[offset] == 1
            offset++
        }
    }
    if version >= 5 {
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

func (m *DescribeGroupsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Groups: The names of the groups to describe
    if version >= 5 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Groups) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Groups)))
    }
    for _, groups := range m.Groups {
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*groups)))
        }
        if groups != nil {
            buff = append(buff, *groups...)
        }
    }
    if version >= 3 {
        // writing m.IncludeAuthorizedOperations: Whether to include authorized operations.
        if m.IncludeAuthorizedOperations {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    if version >= 5 {
        numTaggedFields2 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields2))
    }
    return buff
}

func (m *DescribeGroupsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Groups: The names of the groups to describe
    if version >= 5 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Groups) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, groups := range m.Groups {
        size += 0 * int(unsafe.Sizeof(groups)) // hack to make sure loop variable is always used
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(*groups) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if groups != nil {
            size += len(*groups)
        }
    }
    if version >= 3 {
        // size for m.IncludeAuthorizedOperations: Whether to include authorized operations.
        size += 1
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 5 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *DescribeGroupsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 5 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *DescribeGroupsRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
