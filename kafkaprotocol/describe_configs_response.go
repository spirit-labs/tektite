// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type DescribeConfigsResponseDescribeConfigsSynonym struct {
    // The synonym name.
    Name *string
    // The synonym value.
    Value *string
    // The synonym source.
    Source int8
}

type DescribeConfigsResponseDescribeConfigsResourceResult struct {
    // The configuration name.
    Name *string
    // The configuration value.
    Value *string
    // True if the configuration is read-only.
    ReadOnly bool
    // True if the configuration is not set.
    IsDefault bool
    // The configuration source.
    ConfigSource int8
    // True if this configuration is sensitive.
    IsSensitive bool
    // The synonyms for this configuration key.
    Synonyms []DescribeConfigsResponseDescribeConfigsSynonym
    // The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
    ConfigType int8
    // The configuration documentation.
    Documentation *string
}

type DescribeConfigsResponseDescribeConfigsResult struct {
    // The error code, or 0 if we were able to successfully describe the configurations.
    ErrorCode int16
    // The error message, or null if we were able to successfully describe the configurations.
    ErrorMessage *string
    // The resource type.
    ResourceType int8
    // The resource name.
    ResourceName *string
    // Each listed configuration.
    Configs []DescribeConfigsResponseDescribeConfigsResourceResult
}

type DescribeConfigsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The results for each resource.
    Results []DescribeConfigsResponseDescribeConfigsResult
}

func (m *DescribeConfigsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.Results: The results for each resource.
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
            results := make([]DescribeConfigsResponseDescribeConfigsResult, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading results[i0].ErrorCode: The error code, or 0 if we were able to successfully describe the configurations.
                    results[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading results[i0].ErrorMessage: The error message, or null if we were able to successfully describe the configurations.
                    if version >= 4 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            results[i0].ErrorMessage = &s
                            offset += l1
                        } else {
                            results[i0].ErrorMessage = nil
                        }
                    } else {
                        // non flexible and nullable
                        var l1 int
                        l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                        offset += 2
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            results[i0].ErrorMessage = &s
                            offset += l1
                        } else {
                            results[i0].ErrorMessage = nil
                        }
                    }
                }
                {
                    // reading results[i0].ResourceType: The resource type.
                    results[i0].ResourceType = int8(buff[offset])
                    offset++
                }
                {
                    // reading results[i0].ResourceName: The resource name.
                    if version >= 4 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        s := string(buff[offset: offset + l2])
                        results[i0].ResourceName = &s
                        offset += l2
                    } else {
                        // non flexible and non nullable
                        var l2 int
                        l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l2])
                        results[i0].ResourceName = &s
                        offset += l2
                    }
                }
                {
                    // reading results[i0].Configs: Each listed configuration.
                    var l3 int
                    if version >= 4 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 = int(u - 1)
                    } else {
                        // non flexible and non nullable
                        l3 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    if l3 >= 0 {
                        // length will be -1 if field is null
                        configs := make([]DescribeConfigsResponseDescribeConfigsResourceResult, l3)
                        for i1 := 0; i1 < l3; i1++ {
                            // reading non tagged fields
                            {
                                // reading configs[i1].Name: The configuration name.
                                if version >= 4 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 := int(u - 1)
                                    s := string(buff[offset: offset + l4])
                                    configs[i1].Name = &s
                                    offset += l4
                                } else {
                                    // non flexible and non nullable
                                    var l4 int
                                    l4 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l4])
                                    configs[i1].Name = &s
                                    offset += l4
                                }
                            }
                            {
                                // reading configs[i1].Value: The configuration value.
                                if version >= 4 {
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l5 := int(u - 1)
                                    if l5 > 0 {
                                        s := string(buff[offset: offset + l5])
                                        configs[i1].Value = &s
                                        offset += l5
                                    } else {
                                        configs[i1].Value = nil
                                    }
                                } else {
                                    // non flexible and nullable
                                    var l5 int
                                    l5 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                    offset += 2
                                    if l5 > 0 {
                                        s := string(buff[offset: offset + l5])
                                        configs[i1].Value = &s
                                        offset += l5
                                    } else {
                                        configs[i1].Value = nil
                                    }
                                }
                            }
                            {
                                // reading configs[i1].ReadOnly: True if the configuration is read-only.
                                configs[i1].ReadOnly = buff[offset] == 1
                                offset++
                            }
                            if version <= 0 {
                                {
                                    // reading configs[i1].IsDefault: True if the configuration is not set.
                                    configs[i1].IsDefault = buff[offset] == 1
                                    offset++
                                }
                            }
                            if version >= 1 {
                                {
                                    // reading configs[i1].ConfigSource: The configuration source.
                                    configs[i1].ConfigSource = int8(buff[offset])
                                    offset++
                                }
                            }
                            {
                                // reading configs[i1].IsSensitive: True if this configuration is sensitive.
                                configs[i1].IsSensitive = buff[offset] == 1
                                offset++
                            }
                            if version >= 1 {
                                {
                                    // reading configs[i1].Synonyms: The synonyms for this configuration key.
                                    var l6 int
                                    if version >= 4 {
                                        // flexible and not nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l6 = int(u - 1)
                                    } else {
                                        // non flexible and non nullable
                                        l6 = int(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                    if l6 >= 0 {
                                        // length will be -1 if field is null
                                        synonyms := make([]DescribeConfigsResponseDescribeConfigsSynonym, l6)
                                        for i2 := 0; i2 < l6; i2++ {
                                            // reading non tagged fields
                                            {
                                                // reading synonyms[i2].Name: The synonym name.
                                                if version >= 4 {
                                                    // flexible and not nullable
                                                    u, n := binary.Uvarint(buff[offset:])
                                                    offset += n
                                                    l7 := int(u - 1)
                                                    s := string(buff[offset: offset + l7])
                                                    synonyms[i2].Name = &s
                                                    offset += l7
                                                } else {
                                                    // non flexible and non nullable
                                                    var l7 int
                                                    l7 = int(binary.BigEndian.Uint16(buff[offset:]))
                                                    offset += 2
                                                    s := string(buff[offset: offset + l7])
                                                    synonyms[i2].Name = &s
                                                    offset += l7
                                                }
                                            }
                                            {
                                                // reading synonyms[i2].Value: The synonym value.
                                                if version >= 4 {
                                                    // flexible and nullable
                                                    u, n := binary.Uvarint(buff[offset:])
                                                    offset += n
                                                    l8 := int(u - 1)
                                                    if l8 > 0 {
                                                        s := string(buff[offset: offset + l8])
                                                        synonyms[i2].Value = &s
                                                        offset += l8
                                                    } else {
                                                        synonyms[i2].Value = nil
                                                    }
                                                } else {
                                                    // non flexible and nullable
                                                    var l8 int
                                                    l8 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                                    offset += 2
                                                    if l8 > 0 {
                                                        s := string(buff[offset: offset + l8])
                                                        synonyms[i2].Value = &s
                                                        offset += l8
                                                    } else {
                                                        synonyms[i2].Value = nil
                                                    }
                                                }
                                            }
                                            {
                                                // reading synonyms[i2].Source: The synonym source.
                                                synonyms[i2].Source = int8(buff[offset])
                                                offset++
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
                                    configs[i1].Synonyms = synonyms
                                    }
                                }
                            }
                            if version >= 3 {
                                {
                                    // reading configs[i1].ConfigType: The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
                                    configs[i1].ConfigType = int8(buff[offset])
                                    offset++
                                }
                                {
                                    // reading configs[i1].Documentation: The configuration documentation.
                                    if version >= 4 {
                                        // flexible and nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l9 := int(u - 1)
                                        if l9 > 0 {
                                            s := string(buff[offset: offset + l9])
                                            configs[i1].Documentation = &s
                                            offset += l9
                                        } else {
                                            configs[i1].Documentation = nil
                                        }
                                    } else {
                                        // non flexible and nullable
                                        var l9 int
                                        l9 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                        offset += 2
                                        if l9 > 0 {
                                            s := string(buff[offset: offset + l9])
                                            configs[i1].Documentation = &s
                                            offset += l9
                                        } else {
                                            configs[i1].Documentation = nil
                                        }
                                    }
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
                    results[i0].Configs = configs
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
        m.Results = results
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

func (m *DescribeConfigsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.Results: The results for each resource.
    if version >= 4 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Results) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Results)))
    }
    for _, results := range m.Results {
        // writing non tagged fields
        // writing results.ErrorCode: The error code, or 0 if we were able to successfully describe the configurations.
        buff = binary.BigEndian.AppendUint16(buff, uint16(results.ErrorCode))
        // writing results.ErrorMessage: The error message, or null if we were able to successfully describe the configurations.
        if version >= 4 {
            // flexible and nullable
            if results.ErrorMessage == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*results.ErrorMessage) + 1))
            }
        } else {
            // non flexible and nullable
            if results.ErrorMessage == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*results.ErrorMessage)))
            }
        }
        if results.ErrorMessage != nil {
            buff = append(buff, *results.ErrorMessage...)
        }
        // writing results.ResourceType: The resource type.
        buff = append(buff, byte(results.ResourceType))
        // writing results.ResourceName: The resource name.
        if version >= 4 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*results.ResourceName) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*results.ResourceName)))
        }
        if results.ResourceName != nil {
            buff = append(buff, *results.ResourceName...)
        }
        // writing results.Configs: Each listed configuration.
        if version >= 4 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(results.Configs) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(results.Configs)))
        }
        for _, configs := range results.Configs {
            // writing non tagged fields
            // writing configs.Name: The configuration name.
            if version >= 4 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*configs.Name) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*configs.Name)))
            }
            if configs.Name != nil {
                buff = append(buff, *configs.Name...)
            }
            // writing configs.Value: The configuration value.
            if version >= 4 {
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
            // writing configs.ReadOnly: True if the configuration is read-only.
            if configs.ReadOnly {
                buff = append(buff, 1)
            } else {
                buff = append(buff, 0)
            }
            if version <= 0 {
                // writing configs.IsDefault: True if the configuration is not set.
                if configs.IsDefault {
                    buff = append(buff, 1)
                } else {
                    buff = append(buff, 0)
                }
            }
            if version >= 1 {
                // writing configs.ConfigSource: The configuration source.
                buff = append(buff, byte(configs.ConfigSource))
            }
            // writing configs.IsSensitive: True if this configuration is sensitive.
            if configs.IsSensitive {
                buff = append(buff, 1)
            } else {
                buff = append(buff, 0)
            }
            if version >= 1 {
                // writing configs.Synonyms: The synonyms for this configuration key.
                if version >= 4 {
                    // flexible and not nullable
                    buff = binary.AppendUvarint(buff, uint64(len(configs.Synonyms) + 1))
                } else {
                    // non flexible and non nullable
                    buff = binary.BigEndian.AppendUint32(buff, uint32(len(configs.Synonyms)))
                }
                for _, synonyms := range configs.Synonyms {
                    // writing non tagged fields
                    // writing synonyms.Name: The synonym name.
                    if version >= 4 {
                        // flexible and not nullable
                        buff = binary.AppendUvarint(buff, uint64(len(*synonyms.Name) + 1))
                    } else {
                        // non flexible and non nullable
                        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*synonyms.Name)))
                    }
                    if synonyms.Name != nil {
                        buff = append(buff, *synonyms.Name...)
                    }
                    // writing synonyms.Value: The synonym value.
                    if version >= 4 {
                        // flexible and nullable
                        if synonyms.Value == nil {
                            // null
                            buff = append(buff, 0)
                        } else {
                            // not null
                            buff = binary.AppendUvarint(buff, uint64(len(*synonyms.Value) + 1))
                        }
                    } else {
                        // non flexible and nullable
                        if synonyms.Value == nil {
                            // null
                            buff = binary.BigEndian.AppendUint16(buff, 65535)
                        } else {
                            // not null
                            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*synonyms.Value)))
                        }
                    }
                    if synonyms.Value != nil {
                        buff = append(buff, *synonyms.Value...)
                    }
                    // writing synonyms.Source: The synonym source.
                    buff = append(buff, byte(synonyms.Source))
                    if version >= 4 {
                        numTaggedFields17 := 0
                        // write number of tagged fields
                        buff = binary.AppendUvarint(buff, uint64(numTaggedFields17))
                    }
                }
            }
            if version >= 3 {
                // writing configs.ConfigType: The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
                buff = append(buff, byte(configs.ConfigType))
                // writing configs.Documentation: The configuration documentation.
                if version >= 4 {
                    // flexible and nullable
                    if configs.Documentation == nil {
                        // null
                        buff = append(buff, 0)
                    } else {
                        // not null
                        buff = binary.AppendUvarint(buff, uint64(len(*configs.Documentation) + 1))
                    }
                } else {
                    // non flexible and nullable
                    if configs.Documentation == nil {
                        // null
                        buff = binary.BigEndian.AppendUint16(buff, 65535)
                    } else {
                        // not null
                        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*configs.Documentation)))
                    }
                }
                if configs.Documentation != nil {
                    buff = append(buff, *configs.Documentation...)
                }
            }
            if version >= 4 {
                numTaggedFields20 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields20))
            }
        }
        if version >= 4 {
            numTaggedFields21 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields21))
        }
    }
    if version >= 4 {
        numTaggedFields22 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields22))
    }
    return buff
}

func (m *DescribeConfigsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.Results: The results for each resource.
    if version >= 4 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Results) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, results := range m.Results {
        size += 0 * int(unsafe.Sizeof(results)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for results.ErrorCode: The error code, or 0 if we were able to successfully describe the configurations.
        size += 2
        // size for results.ErrorMessage: The error message, or null if we were able to successfully describe the configurations.
        if version >= 4 {
            // flexible and nullable
            if results.ErrorMessage == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*results.ErrorMessage) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if results.ErrorMessage != nil {
            size += len(*results.ErrorMessage)
        }
        // size for results.ResourceType: The resource type.
        size += 1
        // size for results.ResourceName: The resource name.
        if version >= 4 {
            // flexible and not nullable
            size += sizeofUvarint(len(*results.ResourceName) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if results.ResourceName != nil {
            size += len(*results.ResourceName)
        }
        // size for results.Configs: Each listed configuration.
        if version >= 4 {
            // flexible and not nullable
            size += sizeofUvarint(len(results.Configs) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, configs := range results.Configs {
            size += 0 * int(unsafe.Sizeof(configs)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for configs.Name: The configuration name.
            if version >= 4 {
                // flexible and not nullable
                size += sizeofUvarint(len(*configs.Name) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if configs.Name != nil {
                size += len(*configs.Name)
            }
            // size for configs.Value: The configuration value.
            if version >= 4 {
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
            // size for configs.ReadOnly: True if the configuration is read-only.
            size += 1
            if version <= 0 {
                // size for configs.IsDefault: True if the configuration is not set.
                size += 1
            }
            if version >= 1 {
                // size for configs.ConfigSource: The configuration source.
                size += 1
            }
            // size for configs.IsSensitive: True if this configuration is sensitive.
            size += 1
            if version >= 1 {
                // size for configs.Synonyms: The synonyms for this configuration key.
                if version >= 4 {
                    // flexible and not nullable
                    size += sizeofUvarint(len(configs.Synonyms) + 1)
                } else {
                    // non flexible and non nullable
                    size += 4
                }
                for _, synonyms := range configs.Synonyms {
                    size += 0 * int(unsafe.Sizeof(synonyms)) // hack to make sure loop variable is always used
                    // calculating size for non tagged fields
                    numTaggedFields3:= 0
                    numTaggedFields3 += 0
                    // size for synonyms.Name: The synonym name.
                    if version >= 4 {
                        // flexible and not nullable
                        size += sizeofUvarint(len(*synonyms.Name) + 1)
                    } else {
                        // non flexible and non nullable
                        size += 2
                    }
                    if synonyms.Name != nil {
                        size += len(*synonyms.Name)
                    }
                    // size for synonyms.Value: The synonym value.
                    if version >= 4 {
                        // flexible and nullable
                        if synonyms.Value == nil {
                            // null
                            size += 1
                        } else {
                            // not null
                            size += sizeofUvarint(len(*synonyms.Value) + 1)
                        }
                    } else {
                        // non flexible and nullable
                        size += 2
                    }
                    if synonyms.Value != nil {
                        size += len(*synonyms.Value)
                    }
                    // size for synonyms.Source: The synonym source.
                    size += 1
                    numTaggedFields4:= 0
                    numTaggedFields4 += 0
                    if version >= 4 {
                        // writing size of num tagged fields field
                        size += sizeofUvarint(numTaggedFields4)
                    }
                }
            }
            if version >= 3 {
                // size for configs.ConfigType: The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD
                size += 1
                // size for configs.Documentation: The configuration documentation.
                if version >= 4 {
                    // flexible and nullable
                    if configs.Documentation == nil {
                        // null
                        size += 1
                    } else {
                        // not null
                        size += sizeofUvarint(len(*configs.Documentation) + 1)
                    }
                } else {
                    // non flexible and nullable
                    size += 2
                }
                if configs.Documentation != nil {
                    size += len(*configs.Documentation)
                }
            }
            numTaggedFields5:= 0
            numTaggedFields5 += 0
            if version >= 4 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields5)
            }
        }
        numTaggedFields6:= 0
        numTaggedFields6 += 0
        if version >= 4 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields6)
        }
    }
    numTaggedFields7:= 0
    numTaggedFields7 += 0
    if version >= 4 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields7)
    }
    return size, tagSizes
}


