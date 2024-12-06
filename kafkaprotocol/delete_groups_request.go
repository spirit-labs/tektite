// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type DeleteGroupsRequest struct {
    // The group names to delete.
    GroupsNames []*string
}

func (m *DeleteGroupsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.GroupsNames: The group names to delete.
        var l0 int
        if version >= 2 {
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
            groupsNames := make([]*string, l0)
            for i0 := 0; i0 < l0; i0++ {
                if version >= 2 {
                    // flexible and not nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l1 := int(u - 1)
                    s := string(buff[offset: offset + l1])
                    groupsNames[i0] = &s
                    offset += l1
                } else {
                    // non flexible and non nullable
                    var l1 int
                    l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                    s := string(buff[offset: offset + l1])
                    groupsNames[i0] = &s
                    offset += l1
                }
            }
            m.GroupsNames = groupsNames
        }
    }
    if version >= 2 {
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

func (m *DeleteGroupsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.GroupsNames: The group names to delete.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.GroupsNames) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.GroupsNames)))
    }
    for _, groupsNames := range m.GroupsNames {
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groupsNames) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*groupsNames)))
        }
        if groupsNames != nil {
            buff = append(buff, *groupsNames...)
        }
    }
    if version >= 2 {
        numTaggedFields1 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields1))
    }
    return buff
}

func (m *DeleteGroupsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.GroupsNames: The group names to delete.
    if version >= 2 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.GroupsNames) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, groupsNames := range m.GroupsNames {
        size += 0 * int(unsafe.Sizeof(groupsNames)) // hack to make sure loop variable is always used
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(*groupsNames) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if groupsNames != nil {
            size += len(*groupsNames)
        }
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *DeleteGroupsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 2 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *DeleteGroupsRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
