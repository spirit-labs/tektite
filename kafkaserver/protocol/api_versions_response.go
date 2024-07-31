// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "unsafe"
import "github.com/spirit-labs/tektite/debug"
import "fmt"

type ApiVersionsResponseApiVersion struct {
    // The API index.
    ApiKey int16
    // The minimum supported version, inclusive.
    MinVersion int16
    // The maximum supported version, inclusive.
    MaxVersion int16
}

type ApiVersionsResponseSupportedFeatureKey struct {
    // The name of the feature.
    Name *string
    // The minimum supported version for the feature.
    MinVersion int16
    // The maximum supported version for the feature.
    MaxVersion int16
}

type ApiVersionsResponseFinalizedFeatureKey struct {
    // The name of the feature.
    Name *string
    // The cluster-wide finalized max version level for the feature.
    MaxVersionLevel int16
    // The cluster-wide finalized min version level for the feature.
    MinVersionLevel int16
}

type ApiVersionsResponse struct {
    // The top-level error code.
    ErrorCode int16
    // The APIs supported by the broker.
    ApiKeys []ApiVersionsResponseApiVersion
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 show up with MinSupportedVersion = 1.
    SupportedFeatures []ApiVersionsResponseSupportedFeatureKey
    // The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
    FinalizedFeaturesEpoch int64
    // List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
    FinalizedFeatures []ApiVersionsResponseFinalizedFeatureKey
    // Set by a KRaft controller if the required configurations for ZK migration are present
    ZkMigrationReady bool
}

func (m *ApiVersionsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ErrorCode: The top-level error code.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.ApiKeys: The APIs supported by the broker.
        var l0 int
        if version >= 3 {
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
            apiKeys := make([]ApiVersionsResponseApiVersion, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading apiKeys[i0].ApiKey: The API index.
                    apiKeys[i0].ApiKey = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading apiKeys[i0].MinVersion: The minimum supported version, inclusive.
                    apiKeys[i0].MinVersion = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading apiKeys[i0].MaxVersion: The maximum supported version, inclusive.
                    apiKeys[i0].MaxVersion = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
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
            }
        m.ApiKeys = apiKeys
        }
    }
    if version >= 1 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
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
                case 0:
                    {
                        // reading m.SupportedFeatures: Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 show up with MinSupportedVersion = 1.
                        var l1 int
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 = int(u - 1)
                        if l1 >= 0 {
                            // length will be -1 if field is null
                            supportedFeatures := make([]ApiVersionsResponseSupportedFeatureKey, l1)
                            for i1 := 0; i1 < l1; i1++ {
                                // reading non tagged fields
                                {
                                    // reading supportedFeatures[i1].Name: The name of the feature.
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l2 := int(u - 1)
                                    s := string(buff[offset: offset + l2])
                                    supportedFeatures[i1].Name = &s
                                    offset += l2
                                }
                                {
                                    // reading supportedFeatures[i1].MinVersion: The minimum supported version for the feature.
                                    supportedFeatures[i1].MinVersion = int16(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                }
                                {
                                    // reading supportedFeatures[i1].MaxVersion: The maximum supported version for the feature.
                                    supportedFeatures[i1].MaxVersion = int16(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
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
                            }
                        m.SupportedFeatures = supportedFeatures
                        }
                    }
                case 1:
                    {
                        // reading m.FinalizedFeaturesEpoch: The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
                        m.FinalizedFeaturesEpoch = int64(binary.BigEndian.Uint64(buff[offset:]))
                        offset += 8
                    }
                case 2:
                    {
                        // reading m.FinalizedFeatures: List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
                        var l3 int
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 = int(u - 1)
                        if l3 >= 0 {
                            // length will be -1 if field is null
                            finalizedFeatures := make([]ApiVersionsResponseFinalizedFeatureKey, l3)
                            for i2 := 0; i2 < l3; i2++ {
                                // reading non tagged fields
                                {
                                    // reading finalizedFeatures[i2].Name: The name of the feature.
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 := int(u - 1)
                                    s := string(buff[offset: offset + l4])
                                    finalizedFeatures[i2].Name = &s
                                    offset += l4
                                }
                                {
                                    // reading finalizedFeatures[i2].MaxVersionLevel: The cluster-wide finalized max version level for the feature.
                                    finalizedFeatures[i2].MaxVersionLevel = int16(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                }
                                {
                                    // reading finalizedFeatures[i2].MinVersionLevel: The cluster-wide finalized min version level for the feature.
                                    finalizedFeatures[i2].MinVersionLevel = int16(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
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
                            }
                        m.FinalizedFeatures = finalizedFeatures
                        }
                    }
                case 3:
                    {
                        // reading m.ZkMigrationReady: Set by a KRaft controller if the required configurations for ZK migration are present
                        m.ZkMigrationReady = buff[offset] == 1
                        offset++
                    }
                default:
                    offset += int(ts)
            }
        }
    }
    return offset, nil
}

func (m *ApiVersionsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ErrorCode: The top-level error code.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.ApiKeys: The APIs supported by the broker.
    if version >= 3 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.ApiKeys) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.ApiKeys)))
    }
    for _, apiKeys := range m.ApiKeys {
        // writing non tagged fields
        // writing apiKeys.ApiKey: The API index.
        buff = binary.BigEndian.AppendUint16(buff, uint16(apiKeys.ApiKey))
        // writing apiKeys.MinVersion: The minimum supported version, inclusive.
        buff = binary.BigEndian.AppendUint16(buff, uint16(apiKeys.MinVersion))
        // writing apiKeys.MaxVersion: The maximum supported version, inclusive.
        buff = binary.BigEndian.AppendUint16(buff, uint16(apiKeys.MaxVersion))
        if version >= 3 {
            numTaggedFields5 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields5))
        }
    }
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    if version >= 3 {
        numTaggedFields7 := 0
        // writing tagged field increments
        // tagged field - m.SupportedFeatures: Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 show up with MinSupportedVersion = 1.
        numTaggedFields7++
        // tagged field - m.FinalizedFeaturesEpoch: The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
        numTaggedFields7++
        // tagged field - m.FinalizedFeatures: List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
        numTaggedFields7++
        // tagged field - m.ZkMigrationReady: Set by a KRaft controller if the required configurations for ZK migration are present
        numTaggedFields7++
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
        // writing tagged fields
        // tag header
        buff = binary.AppendUvarint(buff, uint64(0))
        buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
        tagPos++
        var tagSizeStart8 int
        if debug.SanityChecks {
            tagSizeStart8 = len(buff)
        }
        // writing m.SupportedFeatures: Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 show up with MinSupportedVersion = 1.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.SupportedFeatures) + 1))
        for _, supportedFeatures := range m.SupportedFeatures {
            // writing non tagged fields
            // writing supportedFeatures.Name: The name of the feature.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*supportedFeatures.Name) + 1))
            if supportedFeatures.Name != nil {
                buff = append(buff, *supportedFeatures.Name...)
            }
            // writing supportedFeatures.MinVersion: The minimum supported version for the feature.
            buff = binary.BigEndian.AppendUint16(buff, uint16(supportedFeatures.MinVersion))
            // writing supportedFeatures.MaxVersion: The maximum supported version for the feature.
            buff = binary.BigEndian.AppendUint16(buff, uint16(supportedFeatures.MaxVersion))
            numTaggedFields12 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields12))
        }
        if debug.SanityChecks && len(buff) - tagSizeStart8 != tagSizes[tagPos - 1] {
            panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 0))
        }
        // tag header
        buff = binary.AppendUvarint(buff, uint64(1))
        buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
        tagPos++
        var tagSizeStart13 int
        if debug.SanityChecks {
            tagSizeStart13 = len(buff)
        }
        // writing m.FinalizedFeaturesEpoch: The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
        buff = binary.BigEndian.AppendUint64(buff, uint64(m.FinalizedFeaturesEpoch))
        if debug.SanityChecks && len(buff) - tagSizeStart13 != tagSizes[tagPos - 1] {
            panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 1))
        }
        // tag header
        buff = binary.AppendUvarint(buff, uint64(2))
        buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
        tagPos++
        var tagSizeStart14 int
        if debug.SanityChecks {
            tagSizeStart14 = len(buff)
        }
        // writing m.FinalizedFeatures: List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.FinalizedFeatures) + 1))
        for _, finalizedFeatures := range m.FinalizedFeatures {
            // writing non tagged fields
            // writing finalizedFeatures.Name: The name of the feature.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*finalizedFeatures.Name) + 1))
            if finalizedFeatures.Name != nil {
                buff = append(buff, *finalizedFeatures.Name...)
            }
            // writing finalizedFeatures.MaxVersionLevel: The cluster-wide finalized max version level for the feature.
            buff = binary.BigEndian.AppendUint16(buff, uint16(finalizedFeatures.MaxVersionLevel))
            // writing finalizedFeatures.MinVersionLevel: The cluster-wide finalized min version level for the feature.
            buff = binary.BigEndian.AppendUint16(buff, uint16(finalizedFeatures.MinVersionLevel))
            numTaggedFields18 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields18))
        }
        if debug.SanityChecks && len(buff) - tagSizeStart14 != tagSizes[tagPos - 1] {
            panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 2))
        }
        // tag header
        buff = binary.AppendUvarint(buff, uint64(3))
        buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
        tagPos++
        var tagSizeStart19 int
        if debug.SanityChecks {
            tagSizeStart19 = len(buff)
        }
        // writing m.ZkMigrationReady: Set by a KRaft controller if the required configurations for ZK migration are present
        if m.ZkMigrationReady {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
        if debug.SanityChecks && len(buff) - tagSizeStart19 != tagSizes[tagPos - 1] {
            panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 3))
        }
    }
    return buff
}

func (m *ApiVersionsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ErrorCode: The top-level error code.
    size += 2
    // size for m.ApiKeys: The APIs supported by the broker.
    if version >= 3 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.ApiKeys) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, apiKeys := range m.ApiKeys {
        size += 0 * int(unsafe.Sizeof(apiKeys)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for apiKeys.ApiKey: The API index.
        size += 2
        // size for apiKeys.MinVersion: The minimum supported version, inclusive.
        size += 2
        // size for apiKeys.MaxVersion: The maximum supported version, inclusive.
        size += 2
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 3 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    if version >= 1 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    taggedFieldStart := 0
    taggedFieldSize := 0
    if version >= 3 {
        // size for m.SupportedFeatures: Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 show up with MinSupportedVersion = 1.
        numTaggedFields3++
        taggedFieldStart = size
        // flexible and not nullable
        size += sizeofUvarint(len(m.SupportedFeatures) + 1)
        for _, supportedFeatures := range m.SupportedFeatures {
            size += 0 * int(unsafe.Sizeof(supportedFeatures)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields4:= 0
            numTaggedFields4 += 0
            // size for supportedFeatures.Name: The name of the feature.
            // flexible and not nullable
            size += sizeofUvarint(len(*supportedFeatures.Name) + 1)
            if supportedFeatures.Name != nil {
                size += len(*supportedFeatures.Name)
            }
            // size for supportedFeatures.MinVersion: The minimum supported version for the feature.
            size += 2
            // size for supportedFeatures.MaxVersion: The maximum supported version for the feature.
            size += 2
            numTaggedFields5:= 0
            numTaggedFields5 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields5)
        }
        taggedFieldSize = size - taggedFieldStart
        tagSizes = append(tagSizes, taggedFieldSize)
        // size = <tag id contrib> + <field size>
        size += sizeofUvarint(0) + sizeofUvarint(taggedFieldSize)
        // size for m.FinalizedFeaturesEpoch: The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
        numTaggedFields3++
        taggedFieldStart = size
        size += 8
        taggedFieldSize = size - taggedFieldStart
        tagSizes = append(tagSizes, taggedFieldSize)
        // size = <tag id contrib> + <field size>
        size += sizeofUvarint(1) + sizeofUvarint(taggedFieldSize)
        // size for m.FinalizedFeatures: List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
        numTaggedFields3++
        taggedFieldStart = size
        // flexible and not nullable
        size += sizeofUvarint(len(m.FinalizedFeatures) + 1)
        for _, finalizedFeatures := range m.FinalizedFeatures {
            size += 0 * int(unsafe.Sizeof(finalizedFeatures)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields6:= 0
            numTaggedFields6 += 0
            // size for finalizedFeatures.Name: The name of the feature.
            // flexible and not nullable
            size += sizeofUvarint(len(*finalizedFeatures.Name) + 1)
            if finalizedFeatures.Name != nil {
                size += len(*finalizedFeatures.Name)
            }
            // size for finalizedFeatures.MaxVersionLevel: The cluster-wide finalized max version level for the feature.
            size += 2
            // size for finalizedFeatures.MinVersionLevel: The cluster-wide finalized min version level for the feature.
            size += 2
            numTaggedFields7:= 0
            numTaggedFields7 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields7)
        }
        taggedFieldSize = size - taggedFieldStart
        tagSizes = append(tagSizes, taggedFieldSize)
        // size = <tag id contrib> + <field size>
        size += sizeofUvarint(2) + sizeofUvarint(taggedFieldSize)
        // size for m.ZkMigrationReady: Set by a KRaft controller if the required configurations for ZK migration are present
        numTaggedFields3++
        taggedFieldStart = size
        size += 1
        taggedFieldSize = size - taggedFieldStart
        tagSizes = append(tagSizes, taggedFieldSize)
        // size = <tag id contrib> + <field size>
        size += sizeofUvarint(3) + sizeofUvarint(taggedFieldSize)
    }
    if version >= 3 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}

