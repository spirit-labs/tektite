// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type ListGroupsRequest struct {
    // The states of the groups we want to list. If empty, all groups are returned with their state.
    StatesFilter []*string
    // The types of the groups we want to list. If empty, all groups are returned with their type.
    TypesFilter []*string
}

func (m *ListGroupsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 4 {
        {
            // reading m.StatesFilter: The states of the groups we want to list. If empty, all groups are returned with their state.
            var l0 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 = int(u - 1)
            if l0 >= 0 {
                // length will be -1 if field is null
                statesFilter := make([]*string, l0)
                for i0 := 0; i0 < l0; i0++ {
                    // flexible and not nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l1 := int(u - 1)
                    s := string(buff[offset: offset + l1])
                    statesFilter[i0] = &s
                    offset += l1
                }
                m.StatesFilter = statesFilter
            }
        }
    }
    if version >= 5 {
        {
            // reading m.TypesFilter: The types of the groups we want to list. If empty, all groups are returned with their type.
            var l2 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l2 = int(u - 1)
            if l2 >= 0 {
                // length will be -1 if field is null
                typesFilter := make([]*string, l2)
                for i1 := 0; i1 < l2; i1++ {
                    // flexible and not nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l3 := int(u - 1)
                    s := string(buff[offset: offset + l3])
                    typesFilter[i1] = &s
                    offset += l3
                }
                m.TypesFilter = typesFilter
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

func (m *ListGroupsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 4 {
        // writing m.StatesFilter: The states of the groups we want to list. If empty, all groups are returned with their state.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.StatesFilter) + 1))
        for _, statesFilter := range m.StatesFilter {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*statesFilter) + 1))
            if statesFilter != nil {
                buff = append(buff, *statesFilter...)
            }
        }
    }
    if version >= 5 {
        // writing m.TypesFilter: The types of the groups we want to list. If empty, all groups are returned with their type.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.TypesFilter) + 1))
        for _, typesFilter := range m.TypesFilter {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*typesFilter) + 1))
            if typesFilter != nil {
                buff = append(buff, *typesFilter...)
            }
        }
    }
    if version >= 3 {
        numTaggedFields2 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields2))
    }
    return buff
}

func (m *ListGroupsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 4 {
        // size for m.StatesFilter: The states of the groups we want to list. If empty, all groups are returned with their state.
        // flexible and not nullable
        size += sizeofUvarint(len(m.StatesFilter) + 1)
        for _, statesFilter := range m.StatesFilter {
            size += 0 * int(unsafe.Sizeof(statesFilter)) // hack to make sure loop variable is always used
            // flexible and not nullable
            size += sizeofUvarint(len(*statesFilter) + 1)
            if statesFilter != nil {
                size += len(*statesFilter)
            }
        }
    }
    if version >= 5 {
        // size for m.TypesFilter: The types of the groups we want to list. If empty, all groups are returned with their type.
        // flexible and not nullable
        size += sizeofUvarint(len(m.TypesFilter) + 1)
        for _, typesFilter := range m.TypesFilter {
            size += 0 * int(unsafe.Sizeof(typesFilter)) // hack to make sure loop variable is always used
            // flexible and not nullable
            size += sizeofUvarint(len(*typesFilter) + 1)
            if typesFilter != nil {
                size += len(*typesFilter)
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

func (m *ListGroupsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 3 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *ListGroupsRequest) SupportedApiVersions() (int16, int16) {
    return 0, 5
}
