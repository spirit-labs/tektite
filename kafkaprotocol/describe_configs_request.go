// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type DescribeConfigsRequestDescribeConfigsResource struct {
    // The resource type.
    ResourceType int8
    // The resource name.
    ResourceName *string
    // The configuration keys to list, or null to list all configuration keys.
    ConfigurationKeys []*string
}

type DescribeConfigsRequest struct {
    // The resources whose configurations we want to describe.
    Resources []DescribeConfigsRequestDescribeConfigsResource
    // True if we should include all synonyms.
    IncludeSynonyms bool
    // True if we should include configuration documentation.
    IncludeDocumentation bool
}

func (m *DescribeConfigsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Resources: The resources whose configurations we want to describe.
        var l0 int
        if version >= 4 {
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
            resources := make([]DescribeConfigsRequestDescribeConfigsResource, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading resources[i0].ResourceType: The resource type.
                    resources[i0].ResourceType = int8(buff[offset])
                    offset++
                }
                {
                    // reading resources[i0].ResourceName: The resource name.
                    if version >= 4 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        s := string(buff[offset: offset + l1])
                        resources[i0].ResourceName = &s
                        offset += l1
                    } else {
                        // non flexible and non nullable
                        var l1 int
                        l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l1])
                        resources[i0].ResourceName = &s
                        offset += l1
                    }
                }
                {
                    // reading resources[i0].ConfigurationKeys: The configuration keys to list, or null to list all configuration keys.
                    var l2 int
                    if version >= 4 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 = int(u - 1)
                    } else {
                        // non flexible and nullable
                        l2 = int(int32(binary.BigEndian.Uint32(buff[offset:])))
                        offset += 4
                    }
                    if l2 >= 0 {
                        // length will be -1 if field is null
                        configurationKeys := make([]*string, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            if version >= 4 {
                                // flexible and nullable
                                u, n := binary.Uvarint(buff[offset:])
                                offset += n
                                l3 := int(u - 1)
                                if l3 > 0 {
                                    s := string(buff[offset: offset + l3])
                                    configurationKeys[i1] = &s
                                    offset += l3
                                } else {
                                    configurationKeys[i1] = nil
                                }
                            } else {
                                // non flexible and nullable
                                var l3 int
                                l3 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                offset += 2
                                if l3 > 0 {
                                    s := string(buff[offset: offset + l3])
                                    configurationKeys[i1] = &s
                                    offset += l3
                                } else {
                                    configurationKeys[i1] = nil
                                }
                            }
                        }
                        resources[i0].ConfigurationKeys = configurationKeys
                    }
                }
                if version >= 4 {
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
        m.Resources = resources
        }
    }
    if version >= 1 {
        {
            // reading m.IncludeSynonyms: True if we should include all synonyms.
            m.IncludeSynonyms = buff[offset] == 1
            offset++
        }
    }
    if version >= 3 {
        {
            // reading m.IncludeDocumentation: True if we should include configuration documentation.
            m.IncludeDocumentation = buff[offset] == 1
            offset++
        }
    }
    if version >= 4 {
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

func (m *DescribeConfigsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Resources: The resources whose configurations we want to describe.
    if version >= 4 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Resources) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Resources)))
    }
    for _, resources := range m.Resources {
        // writing non tagged fields
        // writing resources.ResourceType: The resource type.
        buff = append(buff, byte(resources.ResourceType))
        // writing resources.ResourceName: The resource name.
        if version >= 4 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*resources.ResourceName) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*resources.ResourceName)))
        }
        if resources.ResourceName != nil {
            buff = append(buff, *resources.ResourceName...)
        }
        // writing resources.ConfigurationKeys: The configuration keys to list, or null to list all configuration keys.
        if version >= 4 {
            // flexible and nullable
            if resources.ConfigurationKeys == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(resources.ConfigurationKeys) + 1))
            }
        } else {
            // non flexible and nullable
            if resources.ConfigurationKeys == nil {
                // null
                buff = binary.BigEndian.AppendUint32(buff, 4294967295)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(resources.ConfigurationKeys)))
            }
        }
        for _, configurationKeys := range resources.ConfigurationKeys {
            if version >= 4 {
                // flexible and nullable
                if configurationKeys == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*configurationKeys) + 1))
                }
            } else {
                // non flexible and nullable
                if configurationKeys == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*configurationKeys)))
                }
            }
            if configurationKeys != nil {
                buff = append(buff, *configurationKeys...)
            }
        }
        if version >= 4 {
            numTaggedFields4 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
        }
    }
    if version >= 1 {
        // writing m.IncludeSynonyms: True if we should include all synonyms.
        if m.IncludeSynonyms {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    if version >= 3 {
        // writing m.IncludeDocumentation: True if we should include configuration documentation.
        if m.IncludeDocumentation {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    if version >= 4 {
        numTaggedFields7 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
    }
    return buff
}

func (m *DescribeConfigsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Resources: The resources whose configurations we want to describe.
    if version >= 4 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Resources) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, resources := range m.Resources {
        size += 0 * int(unsafe.Sizeof(resources)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for resources.ResourceType: The resource type.
        size += 1
        // size for resources.ResourceName: The resource name.
        if version >= 4 {
            // flexible and not nullable
            size += sizeofUvarint(len(*resources.ResourceName) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if resources.ResourceName != nil {
            size += len(*resources.ResourceName)
        }
        // size for resources.ConfigurationKeys: The configuration keys to list, or null to list all configuration keys.
        if version >= 4 {
            // flexible and nullable
            if resources.ConfigurationKeys == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(resources.ConfigurationKeys) + 1)
            }
        } else {
            // non flexible and nullable
            size += 4
        }
        for _, configurationKeys := range resources.ConfigurationKeys {
            size += 0 * int(unsafe.Sizeof(configurationKeys)) // hack to make sure loop variable is always used
            if version >= 4 {
                // flexible and nullable
                if configurationKeys == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*configurationKeys) + 1)
                }
            } else {
                // non flexible and nullable
                size += 2
            }
            if configurationKeys != nil {
                size += len(*configurationKeys)
            }
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 4 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    if version >= 1 {
        // size for m.IncludeSynonyms: True if we should include all synonyms.
        size += 1
    }
    if version >= 3 {
        // size for m.IncludeDocumentation: True if we should include configuration documentation.
        size += 1
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 4 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}

func (m *DescribeConfigsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 4 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *DescribeConfigsRequest) SupportedApiVersions() (int16, int16) {
    return 1, 1
}
