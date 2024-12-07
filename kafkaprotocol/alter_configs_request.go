// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type AlterConfigsRequestAlterableConfig struct {
    // The configuration key name.
    Name *string
    // The value to set for the configuration key.
    Value *string
}

type AlterConfigsRequestAlterConfigsResource struct {
    // The resource type.
    ResourceType int8
    // The resource name.
    ResourceName *string
    // The configurations.
    Configs []AlterConfigsRequestAlterableConfig
}

type AlterConfigsRequest struct {
    // The updates for each resource.
    Resources []AlterConfigsRequestAlterConfigsResource
    // True if we should validate the request, but not change the configurations.
    ValidateOnly bool
}

func (m *AlterConfigsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Resources: The updates for each resource.
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
            resources := make([]AlterConfigsRequestAlterConfigsResource, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading resources[i0].ResourceType: The resource type.
                    resources[i0].ResourceType = int8(buff[offset])
                    offset++
                }
                {
                    // reading resources[i0].ResourceName: The resource name.
                    if version >= 2 {
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
                    // reading resources[i0].Configs: The configurations.
                    var l2 int
                    if version >= 2 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 = int(u - 1)
                    } else {
                        // non flexible and non nullable
                        l2 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    if l2 >= 0 {
                        // length will be -1 if field is null
                        configs := make([]AlterConfigsRequestAlterableConfig, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading configs[i1].Name: The configuration key name.
                                if version >= 2 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l3 := int(u - 1)
                                    s := string(buff[offset: offset + l3])
                                    configs[i1].Name = &s
                                    offset += l3
                                } else {
                                    // non flexible and non nullable
                                    var l3 int
                                    l3 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l3])
                                    configs[i1].Name = &s
                                    offset += l3
                                }
                            }
                            {
                                // reading configs[i1].Value: The value to set for the configuration key.
                                if version >= 2 {
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 := int(u - 1)
                                    if l4 > 0 {
                                        s := string(buff[offset: offset + l4])
                                        configs[i1].Value = &s
                                        offset += l4
                                    } else {
                                        configs[i1].Value = nil
                                    }
                                } else {
                                    // non flexible and nullable
                                    var l4 int
                                    l4 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                    offset += 2
                                    if l4 > 0 {
                                        s := string(buff[offset: offset + l4])
                                        configs[i1].Value = &s
                                        offset += l4
                                    } else {
                                        configs[i1].Value = nil
                                    }
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
                        }
                    resources[i0].Configs = configs
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
            }
        m.Resources = resources
        }
    }
    {
        // reading m.ValidateOnly: True if we should validate the request, but not change the configurations.
        m.ValidateOnly = buff[offset] == 1
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
    return offset, nil
}

func (m *AlterConfigsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Resources: The updates for each resource.
    if version >= 2 {
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
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*resources.ResourceName) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*resources.ResourceName)))
        }
        if resources.ResourceName != nil {
            buff = append(buff, *resources.ResourceName...)
        }
        // writing resources.Configs: The configurations.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(resources.Configs) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(resources.Configs)))
        }
        for _, configs := range resources.Configs {
            // writing non tagged fields
            // writing configs.Name: The configuration key name.
            if version >= 2 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*configs.Name) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*configs.Name)))
            }
            if configs.Name != nil {
                buff = append(buff, *configs.Name...)
            }
            // writing configs.Value: The value to set for the configuration key.
            if version >= 2 {
                // flexible and nullable
                if configs.Value == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*configs.Value) + 1))
                }
            } else {
                // non flexible and nullable
                if configs.Value == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*configs.Value)))
                }
            }
            if configs.Value != nil {
                buff = append(buff, *configs.Value...)
            }
            if version >= 2 {
                numTaggedFields6 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields6))
            }
        }
        if version >= 2 {
            numTaggedFields7 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
        }
    }
    // writing m.ValidateOnly: True if we should validate the request, but not change the configurations.
    if m.ValidateOnly {
        buff = append(buff, 1)
    } else {
        buff = append(buff, 0)
    }
    if version >= 2 {
        numTaggedFields9 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
    }
    return buff
}

func (m *AlterConfigsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Resources: The updates for each resource.
    if version >= 2 {
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
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(*resources.ResourceName) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if resources.ResourceName != nil {
            size += len(*resources.ResourceName)
        }
        // size for resources.Configs: The configurations.
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(resources.Configs) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, configs := range resources.Configs {
            size += 0 * int(unsafe.Sizeof(configs)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for configs.Name: The configuration key name.
            if version >= 2 {
                // flexible and not nullable
                size += sizeofUvarint(len(*configs.Name) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if configs.Name != nil {
                size += len(*configs.Name)
            }
            // size for configs.Value: The value to set for the configuration key.
            if version >= 2 {
                // flexible and nullable
                if configs.Value == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*configs.Value) + 1)
                }
            } else {
                // non flexible and nullable
                size += 2
            }
            if configs.Value != nil {
                size += len(*configs.Value)
            }
            numTaggedFields3:= 0
            numTaggedFields3 += 0
            if version >= 2 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        numTaggedFields4:= 0
        numTaggedFields4 += 0
        if version >= 2 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    // size for m.ValidateOnly: True if we should validate the request, but not change the configurations.
    size += 1
    numTaggedFields5:= 0
    numTaggedFields5 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields5)
    }
    return size, tagSizes
}

func (m *AlterConfigsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 2 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *AlterConfigsRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
