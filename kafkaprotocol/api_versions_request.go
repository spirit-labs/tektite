// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"

type ApiVersionsRequest struct {
    // The name of the client.
    ClientSoftwareName *string
    // The version of the client.
    ClientSoftwareVersion *string
}

func (m *ApiVersionsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 3 {
        {
            // reading m.ClientSoftwareName: The name of the client.
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            s := string(buff[offset: offset + l0])
            m.ClientSoftwareName = &s
            offset += l0
        }
        {
            // reading m.ClientSoftwareVersion: The version of the client.
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 := int(u - 1)
            s := string(buff[offset: offset + l1])
            m.ClientSoftwareVersion = &s
            offset += l1
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

func (m *ApiVersionsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 3 {
        // writing m.ClientSoftwareName: The name of the client.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.ClientSoftwareName) + 1))
        if m.ClientSoftwareName != nil {
            buff = append(buff, *m.ClientSoftwareName...)
        }
        // writing m.ClientSoftwareVersion: The version of the client.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.ClientSoftwareVersion) + 1))
        if m.ClientSoftwareVersion != nil {
            buff = append(buff, *m.ClientSoftwareVersion...)
        }
    }
    if version >= 3 {
        numTaggedFields2 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields2))
    }
    return buff
}

func (m *ApiVersionsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 3 {
        // size for m.ClientSoftwareName: The name of the client.
        // flexible and not nullable
        size += sizeofUvarint(len(*m.ClientSoftwareName) + 1)
        if m.ClientSoftwareName != nil {
            size += len(*m.ClientSoftwareName)
        }
        // size for m.ClientSoftwareVersion: The version of the client.
        // flexible and not nullable
        size += sizeofUvarint(len(*m.ClientSoftwareVersion) + 1)
        if m.ClientSoftwareVersion != nil {
            size += len(*m.ClientSoftwareVersion)
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

func (m *ApiVersionsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 3 {
        // ApiVersionsResponse always includes a v0 header.
        // See KIP-511 for details.
        return 2, 0
    } else {
        return 1, 0
    }
}

func (m *ApiVersionsRequest) SupportedApiVersions() (int16, int16) {
    return 0, 4
}
