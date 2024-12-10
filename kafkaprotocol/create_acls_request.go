// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type CreateAclsRequestAclCreation struct {
    // The type of the resource.
    ResourceType int8
    // The resource name for the ACL.
    ResourceName *string
    // The pattern type for the ACL.
    ResourcePatternType int8
    // The principal for the ACL.
    Principal *string
    // The host for the ACL.
    Host *string
    // The operation type for the ACL (read, write, etc.).
    Operation int8
    // The permission type for the ACL (allow, deny, etc.).
    PermissionType int8
}

type CreateAclsRequest struct {
    // The ACLs that we want to create.
    Creations []CreateAclsRequestAclCreation
}

func (m *CreateAclsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Creations: The ACLs that we want to create.
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
            creations := make([]CreateAclsRequestAclCreation, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading creations[i0].ResourceType: The type of the resource.
                    creations[i0].ResourceType = int8(buff[offset])
                    offset++
                }
                {
                    // reading creations[i0].ResourceName: The resource name for the ACL.
                    if version >= 2 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        s := string(buff[offset: offset + l1])
                        creations[i0].ResourceName = &s
                        offset += l1
                    } else {
                        // non flexible and non nullable
                        var l1 int
                        l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l1])
                        creations[i0].ResourceName = &s
                        offset += l1
                    }
                }
                if version >= 1 {
                    {
                        // reading creations[i0].ResourcePatternType: The pattern type for the ACL.
                        creations[i0].ResourcePatternType = int8(buff[offset])
                        offset++
                    }
                }
                {
                    // reading creations[i0].Principal: The principal for the ACL.
                    if version >= 2 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        s := string(buff[offset: offset + l2])
                        creations[i0].Principal = &s
                        offset += l2
                    } else {
                        // non flexible and non nullable
                        var l2 int
                        l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l2])
                        creations[i0].Principal = &s
                        offset += l2
                    }
                }
                {
                    // reading creations[i0].Host: The host for the ACL.
                    if version >= 2 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 := int(u - 1)
                        s := string(buff[offset: offset + l3])
                        creations[i0].Host = &s
                        offset += l3
                    } else {
                        // non flexible and non nullable
                        var l3 int
                        l3 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l3])
                        creations[i0].Host = &s
                        offset += l3
                    }
                }
                {
                    // reading creations[i0].Operation: The operation type for the ACL (read, write, etc.).
                    creations[i0].Operation = int8(buff[offset])
                    offset++
                }
                {
                    // reading creations[i0].PermissionType: The permission type for the ACL (allow, deny, etc.).
                    creations[i0].PermissionType = int8(buff[offset])
                    offset++
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
            }
        m.Creations = creations
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

func (m *CreateAclsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Creations: The ACLs that we want to create.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Creations) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Creations)))
    }
    for _, creations := range m.Creations {
        // writing non tagged fields
        // writing creations.ResourceType: The type of the resource.
        buff = append(buff, byte(creations.ResourceType))
        // writing creations.ResourceName: The resource name for the ACL.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*creations.ResourceName) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*creations.ResourceName)))
        }
        if creations.ResourceName != nil {
            buff = append(buff, *creations.ResourceName...)
        }
        if version >= 1 {
            // writing creations.ResourcePatternType: The pattern type for the ACL.
            buff = append(buff, byte(creations.ResourcePatternType))
        }
        // writing creations.Principal: The principal for the ACL.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*creations.Principal) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*creations.Principal)))
        }
        if creations.Principal != nil {
            buff = append(buff, *creations.Principal...)
        }
        // writing creations.Host: The host for the ACL.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*creations.Host) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*creations.Host)))
        }
        if creations.Host != nil {
            buff = append(buff, *creations.Host...)
        }
        // writing creations.Operation: The operation type for the ACL (read, write, etc.).
        buff = append(buff, byte(creations.Operation))
        // writing creations.PermissionType: The permission type for the ACL (allow, deny, etc.).
        buff = append(buff, byte(creations.PermissionType))
        if version >= 2 {
            numTaggedFields8 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields8))
        }
    }
    if version >= 2 {
        numTaggedFields9 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
    }
    return buff
}

func (m *CreateAclsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Creations: The ACLs that we want to create.
    if version >= 2 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Creations) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, creations := range m.Creations {
        size += 0 * int(unsafe.Sizeof(creations)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for creations.ResourceType: The type of the resource.
        size += 1
        // size for creations.ResourceName: The resource name for the ACL.
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(*creations.ResourceName) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if creations.ResourceName != nil {
            size += len(*creations.ResourceName)
        }
        if version >= 1 {
            // size for creations.ResourcePatternType: The pattern type for the ACL.
            size += 1
        }
        // size for creations.Principal: The principal for the ACL.
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(*creations.Principal) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if creations.Principal != nil {
            size += len(*creations.Principal)
        }
        // size for creations.Host: The host for the ACL.
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(*creations.Host) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if creations.Host != nil {
            size += len(*creations.Host)
        }
        // size for creations.Operation: The operation type for the ACL (read, write, etc.).
        size += 1
        // size for creations.PermissionType: The permission type for the ACL (allow, deny, etc.).
        size += 1
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 2 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}

func (m *CreateAclsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 2 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *CreateAclsRequest) SupportedApiVersions() (int16, int16) {
    return 3, 3
}
