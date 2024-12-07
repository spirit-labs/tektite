// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"

type DescribeClusterRequest struct {
    // Whether to include cluster authorized operations.
    IncludeClusterAuthorizedOperations bool
    // The endpoint type to describe. 1=brokers, 2=controllers.
    EndpointType int8
}

func (m *DescribeClusterRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.IncludeClusterAuthorizedOperations: Whether to include cluster authorized operations.
        m.IncludeClusterAuthorizedOperations = buff[offset] == 1
        offset++
    }
    if version >= 1 {
        {
            // reading m.EndpointType: The endpoint type to describe. 1=brokers, 2=controllers.
            m.EndpointType = int8(buff[offset])
            offset++
        }
    }
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
    return offset, nil
}

func (m *DescribeClusterRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.IncludeClusterAuthorizedOperations: Whether to include cluster authorized operations.
    if m.IncludeClusterAuthorizedOperations {
        buff = append(buff, 1)
    } else {
        buff = append(buff, 0)
    }
    if version >= 1 {
        // writing m.EndpointType: The endpoint type to describe. 1=brokers, 2=controllers.
        buff = append(buff, byte(m.EndpointType))
    }
    numTaggedFields2 := 0
    // write number of tagged fields
    buff = binary.AppendUvarint(buff, uint64(numTaggedFields2))
    return buff
}

func (m *DescribeClusterRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.IncludeClusterAuthorizedOperations: Whether to include cluster authorized operations.
    size += 1
    if version >= 1 {
        // size for m.EndpointType: The endpoint type to describe. 1=brokers, 2=controllers.
        size += 1
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    // writing size of num tagged fields field
    size += sizeofUvarint(numTaggedFields1)
    return size, tagSizes
}

func (m *DescribeClusterRequest) HeaderVersions(version int16) (int16, int16) {
    return 2, 1
}

func (m *DescribeClusterRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
