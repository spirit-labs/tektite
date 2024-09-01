package kafkagen

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/kafkaserver/protocol"
	"github.com/yosuke-furukawa/json5/encoding/json5"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var included = []string{
	"RequestHeader",
	"ResponseHeader",
	"ProduceRequest",
	"ProduceResponse",
	"FetchRequest",
	"FetchResponse",
	"ListOffsetsRequest",
	"ListOffsetsResponse",
	"MetadataRequest",
	"MetadataResponse",
	"OffsetCommitRequest",
	"OffsetCommitResponse",
	"OffsetFetchRequest",
	"OffsetFetchResponse",
	"FindCoordinatorRequest",
	"FindCoordinatorResponse",
	"JoinGroupRequest",
	"JoinGroupResponse",
	"HeartbeatRequest",
	"LeaveGroupRequest",
	"LeaveGroupResponse",
	"SyncGroupRequest",
	"SyncGroupResponse",
	"HeartbeatResponse",
	"ApiVersionsRequest",
	"ApiVersionsResponse",
	"InitProducerIdRequest",
	"InitProducerIdResponse",
	"SaslAuthenticateRequest",
	"SaslAuthenticateResponse",
	"SaslHandshakeRequest",
	"SaslHandshakeResponse",
}

func Generate(specDir string, outDir string) error {
	var specs []MessageSpec
	for _, messageName := range included {
		fileName := fmt.Sprintf("%s/%s.json", specDir, messageName)
		ms, err := loadMessageSpec(fileName)
		if err != nil {
			return err
		}
		specs = append(specs, ms)
		src, err := generateMessage(ms)
		if err != nil {
			return err
		}
		goFileName := fmt.Sprintf("%s/%s", outDir, toSnakeCase(messageName)+".go")
		if err := os.WriteFile(goFileName, []byte(src), 0644); err != nil {
			return err
		}
	}
	bufHandlerStr, err := generateHandler(specs)
	if err != nil {
		return err
	}
	handlerFileName := fmt.Sprintf("%s/%s", outDir, "handler.go")
	return os.WriteFile(handlerFileName, []byte(bufHandlerStr), 0644)
}

func toSnakeCase(str string) string {
	// Match one uppercase letter followed by lowercase letters
	camelCaseRegexp := regexp.MustCompile("(.)([A-Z][a-z]+)")
	snakeCase := camelCaseRegexp.ReplaceAllString(str, "${1}_${2}")
	// Match lowercase letter followed by uppercase letter
	camelCaseRegexp = regexp.MustCompile("([a-z0-9])([A-Z])")
	snakeCase = camelCaseRegexp.ReplaceAllString(snakeCase, "${1}_${2}")
	return strings.ToLower(snakeCase)
}

func generateHandler(specs []MessageSpec) (string, error) {
	var sbBufHandler strings.Builder
	head :=
		`// Package protocol - This is a generated file, please do not edit
package protocol

import (
    "encoding/binary"
    "fmt"
    "github.com/pkg/errors"
    "net"
)

func HandleRequestBuffer(apiKey int16, buff []byte, handler RequestHandler, conn net.Conn) error {
    apiVersion := int16(binary.BigEndian.Uint16(buff[2:]))
    var err error
    var responseHeader ResponseHeader
    switch apiKey {
`
	sbBufHandler.WriteString(head)
	var sbReqHandler strings.Builder
	sbReqHandler.WriteString("\ntype RequestHandler interface {\n")

	for _, ms := range specs {
		if ms.MessageType != "request" {
			continue
		}
		c :=
			`    case %d:
        var req %s
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *%s) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.%sErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("%s", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.Handle%s(&requestHeader, &req, respFunc)   
        }
`
		messageName := ms.Name
		responseName := messageName[:len(messageName)-len("Request")] + "Response"
		sbBufHandler.WriteString(fmt.Sprintf(c, ms.ApiKey, ms.Name, responseName, ms.Name, "version %d for apiKey %d is unsupported. supported versions are %d to %d", ms.Name))
		if !strings.HasSuffix(messageName, "Request") {
			panic(fmt.Sprintf("not a Request: %s", messageName))
		}
		sbReqHandler.WriteString(fmt.Sprintf("    Handle%s(hdr *RequestHeader, req *%s, completionFunc func(resp *%s) error) error\n", messageName,
			messageName, responseName))
		sbReqHandler.WriteString(fmt.Sprintf("    %sErrorResponse(errorCode int16, errorMsg string, req *%s) *%s\n", messageName,
			messageName, responseName))
	}
	sbBufHandler.WriteString("    default: return errors.Errorf(\"Unsupported ApiKey: %d\", apiKey)\n")
	sbBufHandler.WriteString("    }\n")
	sbBufHandler.WriteString("    return err\n")
	sbBufHandler.WriteString("}\n")
	sbReqHandler.WriteString("}\n")
	sbBufHandler.WriteString(sbReqHandler.String())
	return sbBufHandler.String(), nil
}

func loadMessageSpec(fileName string) (MessageSpec, error) {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return MessageSpec{}, err
	}
	var ms MessageSpec
	if err := json5.Unmarshal(bytes, &ms); err != nil {
		return MessageSpec{}, err
	}
	return ms, nil
}

func generateMessage(ms MessageSpec) (string, error) {
	var flexibleRange *versionRange
	if ms.FlexibleVersions != "" && ms.FlexibleVersions != "none" {
		rng, err := parseVersionRange(ms.FlexibleVersions)
		if err != nil {
			return "", err
		}
		flexibleRange = &rng
	}
	gc := &genContext{
		flexibleRange: flexibleRange,
		imports:       map[string]struct{}{},
		commonStructs: make(map[string]MessageField),
		spec:          ms,
	}
	for _, commonStruct := range ms.CommonStructs {
		gc.commonStructs[fmt.Sprintf("%s%s", ms.Name, commonStruct.Name)] = commonStruct
	}
	rewriteStructTypes(ms.Name, ms.Fields, gc)
	for i := 0; i < len(ms.CommonStructs); i++ {
		ms.CommonStructs[i].Name = fmt.Sprintf("%s%s", ms.Name, ms.CommonStructs[i].Name)
		rewriteStructTypes(ms.Name, ms.CommonStructs[i].Fields, gc)
	}
	for _, commonStruct := range ms.CommonStructs {
		if err := generateStructs(commonStruct.Name, commonStruct.Fields, gc); err != nil {
			return "", err
		}
	}
	if err := generateStructs(ms.Name, ms.Fields, gc); err != nil {
		return "", err
	}
	if err := generateRead(&ms, gc); err != nil {
		return "", err
	}
	gc.write("\n")
	if err := generateWrite(&ms, gc); err != nil {
		return "", err
	}
	gc.write("\n")
	if err := generateCalcSize(&ms, gc); err != nil {
		return "", err
	}
	gc.write("\n")
	if err := generateHeaderVersions(&ms, gc); err != nil {
		return "", err
	}
	gc.write("\n")
	if err := generateSupportedApiVersions(&ms, gc); err != nil {
		return "", err
	}
	return gc.string(), nil
}

// Different messages have nested structs with same name, so if we want them to all live in the same package we
// have to rewrite the nested struct types so they are prefixed with the message name
func rewriteStructTypes(messageName string, fields []MessageField, gc *genContext) {
	for i := 0; i < len(fields); i++ {
		childFields := fields[i].Fields
		if childFields == nil {
			common, ok := gc.commonStructs[fmt.Sprintf("%s%s", messageName, fields[i].componentType())]
			if ok {
				childFields = common.Fields
			}
		}
		if childFields != nil {
			var fieldType string
			if fields[i].isArray() {
				fieldType = fmt.Sprintf("[]%s%s", messageName, fields[i].componentType())
			} else {
				fieldType = fmt.Sprintf("%s%s", messageName, fields[i].FieldType)
			}
			fields[i].FieldType = fieldType
			rewriteStructTypes(messageName, childFields, gc)
		}
	}
}

func generateStructs(name string, fields []MessageField, gc *genContext) error {
	// Generate any nested structs first
	for _, field := range fields {

		if field.Fields != nil {
			// Nested struct
			var nestedType string
			if field.isArray() {
				// Array type
				nestedType = field.componentType()
			} else {
				nestedType = field.FieldType
			}
			// We prefix each nested type with the name of the message becomes there are multiple nested types with
			// the same name for different messages and we want to generate everything into the same package
			if err := generateStructs(nestedType, field.Fields, gc); err != nil {
				return err
			}
		}
	}
	gc.writeF("type %s struct {\n", name)
	gc.incIndent()
	for _, field := range fields {
		var fieldType string
		if field.isArray() {
			fieldType = "[]" + parseType(field.componentType())
		} else {
			fieldType = parseType(field.FieldType)
		}
		if field.About != "" {
			gc.writeF("// %s\n", field.About)
		}
		gc.writeF("%s %s\n", field.Name, fieldType)
	}
	gc.decIndent()
	gc.write("}\n\n")
	return nil
}

func generateRead(ms *MessageSpec, gc *genContext) error {
	gc.varSeq = 0
	gc.writeF("func (m *%s) Read(version int16, buff []byte) (int, error) {\n", ms.Name)
	gc.incIndent()
	gc.write("offset := 0\n")
	if err := genReadForFields("m", ms.Fields, gc); err != nil {
		return err
	}
	gc.write("return offset, nil\n")
	gc.decIndent()
	gc.write("}\n")
	return nil
}

func generateWrite(ms *MessageSpec, gc *genContext) error {
	gc.varSeq = 0
	gc.writeF("func (m *%s) Write(version int16, buff []byte, tagSizes []int) []byte {\n", ms.Name)
	gc.incIndent()
	gc.write("var tagPos int\n")
	gc.write("tagPos += 0 // make sure variable is used\n")
	if err := genWriteForFields("m", ms.Fields, gc); err != nil {
		return err
	}
	gc.write("return buff\n")
	gc.decIndent()
	gc.write("}\n")
	return nil
}

func generateCalcSize(ms *MessageSpec, gc *genContext) error {
	gc.varSeq = 0
	gc.writeF("func (m *%s) CalcSize(version int16, tagSizes []int) (int, []int) {\n", ms.Name)
	gc.incIndent()
	gc.write("size := 0\n")
	if err := genCalcForFields("m", ms.Fields, gc); err != nil {
		return err
	}
	gc.write("return size, tagSizes\n")
	gc.decIndent()
	gc.write("}\n")
	return nil
}

func genCalcForFields(structName string, fields []MessageField, gc *genContext) error {
	nonTaggedFields, taggedFields := splitTaggedNonTagged(fields)
	gc.write("// calculating size for non tagged fields\n")
	// First we write out the non tagged fields
	if err := genCalcSize(false, structName, nonTaggedFields, gc); err != nil {
		return err
	}
	// Then tagged fields
	return genCalcSize(true, structName, taggedFields, gc)
}

func generateHeaderVersions(ms *MessageSpec, gc *genContext) error {
	if !strings.HasSuffix(ms.Name, "Request") {
		return nil
	}
	gc.writeF("func (m *%s) HeaderVersions(version int16) (int16, int16) {\n", ms.Name)
	gc.incIndent()
	if ms.Name == "ControlledShutdownRequest" {
		gc.write("if version == 0 {")
		gc.write("    // special case for this message type - only message that uses RequestHeader version 0\n")
		gc.write("    return 0, 0\n")
		gc.write("}\n")
	}
	if gc.flexibleRange == nil {
		gc.write("return 1, 0\n")
	} else if gc.startVersionIf(*gc.flexibleRange) {
		// if version is flexible then we use version 2 of the request header and version 1 of the response header
		// See https://cwiki.apache.org/confluence/display/KAFKA/KIP-482
		if ms.Name == "ApiVersionsRequest" {
			gc.write("// ApiVersionsResponse always includes a v0 header.\n")
			gc.write("// See KIP-511 for details.\n")
			gc.write("return 2, 0\n")
		} else {
			gc.write("return 2, 1\n")
		}
		if gc.startVersionElse() {
			gc.write("return 1, 0\n")
		}
		gc.closeVersionIf()
	}
	gc.decIndent()
	gc.write("}\n")
	return nil
}

func generateSupportedApiVersions(ms *MessageSpec, gc *genContext) error {
	if !strings.HasSuffix(ms.Name, "Request") {
		return nil
	}
	gc.writeF("func (m *%s) SupportedApiVersions() (int16, int16) {\n", ms.Name)
	minVer, maxVer, ok := supportedVersions(int16(ms.ApiKey))
	if !ok {
		return errors.Errorf("No SupportedAPIVersions entry for ApiKey: %d", ms.ApiKey)
	}
	gc.writeF("    return %d, %d\n", minVer, maxVer)
	gc.write("}\n")
	return nil
}

func supportedVersions(apiKey int16) (int16, int16, bool) {
	for _, ver := range protocol.SupportedAPIVersions {
		if ver.ApiKey == apiKey {
			return ver.MinVersion, ver.MaxVersion, true
		}
	}
	return 0, 0, false
}

func splitTaggedNonTagged(fields []MessageField) ([]MessageField, []MessageField) {
	// Separate fields into non tagged and tagged
	var nonTaggedFields []MessageField
	var taggedFields []MessageField
	for _, field := range fields {
		if field.TaggedVersions != "" {
			taggedFields = append(taggedFields, field)
		} else {
			nonTaggedFields = append(nonTaggedFields, field)
		}
	}
	return nonTaggedFields, taggedFields
}

func genReadForFields(structName string, fields []MessageField, gc *genContext) error {
	nonTaggedFields, taggedFields := splitTaggedNonTagged(fields)

	//gc.incNumTagsIndex()
	gc.write("// reading non tagged fields\n")
	// First we write out the non tagged fields
	if err := genReadFields(structName, nonTaggedFields, gc); err != nil {
		return err
	}

	// Read the tagged fields
	if gc.flexibleRange != nil {
		if gc.startVersionIf(*gc.flexibleRange) {
			gc.write("// reading tagged fields\n")
			gc.write("nt, n := binary.Uvarint(buff[offset:])\n")
			gc.write("offset += n\n")
			gc.write("for i := 0; i < int(nt); i++ {\n")
			gc.incIndent()
			gc.write("t, n := binary.Uvarint(buff[offset:])\n")
			gc.write("offset += n\n")
			gc.write("ts, n := binary.Uvarint(buff[offset:])\n")
			gc.write("offset += n\n")
			gc.write("switch t {\n")
			gc.incIndent()
			for _, field := range taggedFields {
				gc.writeF("case %d:\n", field.Tag)
				gc.incIndent()
				taggedVersions, err := parseVersionRange(field.TaggedVersions)
				if err != nil {
					return err
				}
				if gc.startVersionIf(taggedVersions) {
					if err := genReadField(structName, field, gc); err != nil {
						return err
					}
				}
				if gc.startVersionElse() {
					gc.writeF("return 0, errors.Errorf(\"Tag %d %s\", version)\n", field.Tag, "is not valid at version %d")
					gc.addImport("github.com/pkg/errors")
				}
				gc.closeVersionIf()
				gc.decIndent()
			}
			gc.write("default:\n")
			gc.incIndent()
			// Unknown tag, skip past it
			gc.writeF("offset += int(ts)\n")
			gc.decIndent()
			gc.decIndent()
			gc.write("}\n")
			gc.decIndent()
			gc.write("}\n")
		}
		gc.closeVersionIf()
	}
	return nil
}

func genReadField(structName string, field MessageField, gc *genContext) error {
	gc.write("{\n")
	gc.incIndent()
	gc.writeF("// reading %s.%s: %s\n", structName, field.Name, field.About)
	fields := field.Fields
	if fields == nil {
		common, ok := gc.commonStructs[field.componentType()]
		if ok {
			fields = common.Fields
		}
	}
	if field.isArray() {
		lengthVar := gc.varName("l")
		gc.writeF("var %s int\n", lengthVar)
		if fields != nil {
			// array field
			if err := genReadFlexField(gc, &field, fmt.Sprintf("%s.%s", structName, field.Name), false, lengthVar); err != nil {
				return err
			}
			gc.writeF("if %s >= 0 {\n", lengthVar)
			gc.incIndent()
			gc.write("// length will be -1 if field is null\n")
			varName := varName(field.Name)
			gc.writeF("%s := make(%s, %s)\n", varName, field.FieldType, lengthVar)
			loopVar := fmt.Sprintf("i%d", gc.loopIndex)
			gc.incLoopIndex()
			gc.writeF("for %s := 0; %s < %s; %s++ {\n", loopVar, loopVar, lengthVar, loopVar)
			gc.incIndent()
			if err := genReadForFields(fmt.Sprintf("%s[%s]", varName, loopVar), fields, gc); err != nil {
				return err
			}
			gc.decIndent()
			gc.write("}\n")
			gc.decIndent()
			gc.writeF("%s.%s = %s\n", structName, field.Name, varName)
			gc.write("}\n")
		} else {
			// can be []int32, []string, etc
			if err := genReadFlexField(gc, &field, fmt.Sprintf("%s.%s", structName, field.Name), false, lengthVar); err != nil {
				return err
			}
			gc.writeF("if %s >= 0 {\n", lengthVar)
			gc.incIndent()
			gc.write("// length will be -1 if field is null\n")
			varName := varName(field.Name)
			gc.writeF("%s := make([]%s, %s)\n", varName, parseType(field.componentType()), lengthVar)
			loopVar := fmt.Sprintf("i%d", gc.loopIndex)
			gc.incLoopIndex()
			gc.writeF("for %s := 0; %s < %s; %s++ {\n", loopVar, loopVar, lengthVar, loopVar)
			gc.incIndent()
			fieldCpy := field
			fieldCpy.FieldType = fieldCpy.componentType() // remove the []
			if err := genReadSimpleField(fmt.Sprintf("%s[%s]", varName, loopVar), fieldCpy, gc); err != nil {
				return err
			}
			gc.decIndent()
			gc.write("}\n")
			gc.writeF("%s.%s = %s\n", structName, field.Name, varName)
			gc.decIndent()
			gc.write("}\n")
		}
	} else {
		if fields != nil {
			// Non array nested struct
			gc.write("{\n")
			gc.incIndent()
			if err := genReadForFields(fmt.Sprintf("%s.%s", structName, field.Name), fields, gc); err != nil {
				return err
			}
			gc.decIndent()
			gc.write("}\n")
		} else {
			if err := genReadSimpleField(fmt.Sprintf("%s.%s", structName, field.Name), field, gc); err != nil {
				return err
			}
		}
	}
	gc.decIndent()
	gc.write("}\n")
	return nil
}

func genReadFields(structName string, fields []MessageField, gc *genContext) error {
	var prevRange *versionRange
	for _, field := range fields {
		validRange, err := parseVersionRange(field.Versions)
		if err != nil {
			return err
		}
		// We don't want to start a new version check block if the version is the same as the previous one
		if prevRange == nil || !prevRange.equal(&validRange) {
			if prevRange != nil {
				// Version has changed, close previous block
				gc.closeVersionIf()
			}
			// Start a new version check
			if !gc.startVersionIf(validRange) {
				// Field not written at version
				gc.closeVersionIf()
				prevRange = nil
				continue
			}
		}
		prevRange = &validRange
		if err := genReadField(structName, field, gc); err != nil {
			return err
		}
	}
	if prevRange != nil {
		gc.closeVersionIf()
	}
	return nil
}

func varName(s string) string {
	return strings.ToLower(s[:1]) + s[1:]
}

func genReadSimpleField(structName string, field MessageField, gc *genContext) error {
	switch field.FieldType {
	case "bool":
		gc.writeF("%s = buff[offset] == 1\n", structName)
		gc.writeF("offset++\n")
	case "int8":
		gc.writeF("%s = int8(buff[offset])\n", structName)
		gc.writeF("offset++\n")
	case "int16":
		gc.writeF("%s = int16(binary.BigEndian.Uint16(buff[offset:]))\n", structName)
		gc.writeF("offset += 2\n")
	case "uint16":
		gc.writeF("%s = binary.BigEndian.Uint16(buff[offset:])\n", structName)
		gc.writeF("offset += 2\n")
	case "int32":
		gc.writeF("%s = int32(binary.BigEndian.Uint32(buff[offset:]))\n", structName)
		gc.writeF("offset += 4\n")
	case "uint32":
		gc.writeF("%s = binary.BigEndian.Uint32(buff[offset:])\n", structName)
		gc.writeF("offset += 4\n")
	case "int64":
		gc.writeF("%s = int64(binary.BigEndian.Uint64(buff[offset:]))\n", structName)
		gc.writeF("offset += 8\n")
	case "float64":
		gc.writeF("%s = math.Float64frombits(binary.BigEndian.Uint64(buff[offset:]))\n", structName)
		gc.writeF("offset += 8\n")
		gc.addImport("math")
	case "string", "bytes", "records":
		if err := genReadFlexField(gc, &field, structName, true, gc.varName("l")); err != nil {
			return err
		}
	case "uuid":
		gc.writeF("%s = common.ByteSliceCopy(buff[offset: offset + 16])\n", structName)
		gc.write("offset += 16\n")
		gc.addImport("github.com/spirit-labs/tektite/common")
	default:
		return errors.Errorf("unexpected type %s", field.FieldType)
	}
	return nil
}

func genReadFlexField(gc *genContext, field *MessageField, structName string, read bool, lengthVarName string) error {
	flexRange, err := gc.flexibleRangeForField(field)
	if err != nil {
		return err
	}
	if flexRange != nil {
		if field.NullableVersions == "" {
			if gc.startVersionIf(*flexRange) {
				genReadFlexibleNotNullable(gc, field, structName, read, lengthVarName)
			}
			if gc.startVersionElse() {
				genReadNotFlexibleNotNullable(gc, field, structName, read, lengthVarName)
			}
			gc.closeVersionIf()
		} else {
			nullableRange, err := parseVersionRange(field.NullableVersions)
			if err != nil {
				return err
			}
			if gc.startVersionIf(*flexRange) {
				if gc.startVersionIf(nullableRange) {
					genReadFlexibleNullable(gc, field, structName, read, lengthVarName)
				}
				if gc.startVersionElse() {
					genReadFlexibleNotNullable(gc, field, structName, read, lengthVarName)
				}
				gc.closeVersionIf()
			}
			if gc.startVersionElse() {
				if gc.startVersionIf(nullableRange) {
					genReadNotFlexibleNullable(gc, field, structName, read, lengthVarName)
				}
				if gc.startVersionElse() {
					genReadNotFlexibleNotNullable(gc, field, structName, read, lengthVarName)
				}
				gc.closeVersionIf()
			}
			gc.closeVersionIf()
		}
	} else {
		if field.NullableVersions == "" {
			genReadNotFlexibleNotNullable(gc, field, structName, read, lengthVarName)
		} else {
			nullableRange, err := parseVersionRange(field.NullableVersions)
			if err != nil {
				return err
			}
			if gc.startVersionIf(nullableRange) {
				genReadNotFlexibleNullable(gc, field, structName, read, lengthVarName)
			}
			if gc.startVersionElse() {
				genReadNotFlexibleNotNullable(gc, field, structName, read, lengthVarName)
			}
			gc.closeVersionIf()
		}
	}
	return nil
}

func genReadData(gc *genContext, field *MessageField, receiverName string, lengthVarName string) {
	switch field.FieldType {
	case "string":
		gc.writeF("s := string(buff[offset: offset + %s])\n", lengthVarName)
		gc.writeF("%s = &s\n", receiverName)
	case "bytes":
		gc.writeF("%s = common.ByteSliceCopy(buff[offset: offset + %s])\n", receiverName, lengthVarName)
		gc.addImport("github.com/spirit-labs/tektite/common")
	case "records":
		gc.writeF("%s = [][]byte{common.ByteSliceCopy(buff[offset: offset + %s])}\n", receiverName, lengthVarName)
		gc.addImport("github.com/spirit-labs/tektite/common")
	default:
		panic(fmt.Sprintf("unexpected type %s", field.FieldType))
	}
	gc.writeF("offset += %s\n", lengthVarName)
}

func genReadFlexibleNotNullable(gc *genContext, field *MessageField, receiverName string, read bool, lengthVarName string) {
	gc.write("// flexible and not nullable\n")
	gc.writeF("u, n := binary.Uvarint(buff[offset:])\n")
	gc.write("offset += n\n")
	if read {
		gc.writeF("%s := int(u - 1)\n", lengthVarName)
	} else {
		gc.writeF("%s = int(u - 1)\n", lengthVarName)
	}
	if read {
		genReadData(gc, field, receiverName, lengthVarName)
	}
}

func genReadFlexibleNullable(gc *genContext, field *MessageField, receiverName string, read bool, lengthVarName string) {
	gc.write("// flexible and nullable\n")
	gc.write("u, n := binary.Uvarint(buff[offset:])\n")
	gc.write("offset += n\n")
	if read {
		gc.writeF("%s := int(u - 1)\n", lengthVarName)
	} else {
		gc.writeF("%s = int(u - 1)\n", lengthVarName)
	}
	if read {
		gc.writeF("if %s > 0 {\n", lengthVarName)
		gc.incIndent()
		genReadData(gc, field, receiverName, lengthVarName)
		gc.decIndent()
		gc.write("} else {\n")
		gc.incIndent()
		gc.writeF("%s = nil\n", receiverName)
		gc.decIndent()
		gc.write("}\n")
	}
}

func genReadNotFlexibleNotNullable(gc *genContext, field *MessageField, receiverName string, read bool, lengthVarName string) {
	gc.write("// non flexible and non nullable\n")
	if read {
		gc.writeF("var %s int\n", lengthVarName)
	}
	if field.FieldType == "string" {
		gc.writeF("%s = int(binary.BigEndian.Uint16(buff[offset:]))\n", lengthVarName)
		gc.write("offset += 2\n")
	} else {
		gc.writeF("%s = int(binary.BigEndian.Uint32(buff[offset:]))\n", lengthVarName)
		gc.write("offset += 4\n")
	}
	if read {
		genReadData(gc, field, receiverName, lengthVarName)
	}
}

func genReadNotFlexibleNullable(gc *genContext, field *MessageField, receiverName string, read bool, lengthVarName string) {
	gc.write("// non flexible and nullable\n")
	if read {
		gc.writeF("var %s int\n", lengthVarName)
	}
	if field.FieldType == "string" {
		gc.writeF("%s = int(int16(binary.BigEndian.Uint16(buff[offset:])))\n", lengthVarName)
		gc.write("offset += 2\n")
	} else {
		gc.writeF("%s = int(int32(binary.BigEndian.Uint32(buff[offset:])))\n", lengthVarName)
		gc.write("offset += 4\n")
	}
	if read {
		gc.writeF("if %s > 0 {\n", lengthVarName)
		gc.incIndent()
		genReadData(gc, field, receiverName, lengthVarName)
		gc.decIndent()
		gc.write("} else {\n")
		gc.incIndent()
		gc.writeF("%s = nil\n", receiverName)
		gc.decIndent()
		gc.write("}\n")
	}
}

func genWriteForFields(structName string, fields []MessageField, gc *genContext) error {
	nonTaggedFields, taggedFields := splitTaggedNonTagged(fields)
	gc.write("// writing non tagged fields\n")
	// First we write out the non tagged fields
	if err := genWriteFields(false, structName, nonTaggedFields, gc); err != nil {
		return err
	}
	// Then tagged fields
	return writeTaggedFields(structName, taggedFields, gc)
}

func writeTaggedFields(structName string, taggedFields []MessageField, gc *genContext) error {
	if gc.flexibleRange != nil {
		numTaggedFieldsVarName := gc.varName("numTaggedFields")
		if gc.startVersionIf(*gc.flexibleRange) {
			gc.writeF("%s := 0\n", numTaggedFieldsVarName)
			if len(taggedFields) > 0 {
				// Then write out the increments for the tagged fields
				gc.write("// writing tagged field increments\n")
				var prevRange *versionRange
				for _, field := range taggedFields {
					if field.TaggedVersions != "" {
						// Tagged fields get serialized at the end - we keep a count of how many there are at the runtime version
						taggedVersionRange, err := parseVersionRange(field.TaggedVersions)
						if err != nil {
							return err
						}
						// We don't want to start a new version check block if the version is the same as the previous one
						if prevRange == nil || !prevRange.equal(&taggedVersionRange) {
							if prevRange != nil {
								// Version has changed, close previous block
								gc.closeVersionIf()
							}
							// Start a new version check
							if !gc.startVersionIf(taggedVersionRange) {
								// If does not need to be written out as is never true
								gc.closeVersionIf()
								prevRange = nil
								continue
							}
						}
						prevRange = &taggedVersionRange
					}
					gc.writeF("// tagged field - %s.%s: %s\n", structName, field.Name, field.About)
					gc.writeF("%s++\n", numTaggedFieldsVarName)
				}
				if prevRange != nil {
					gc.closeVersionIf()
				}
			}
			// Write out the number of tagged fields
			gc.write("// write number of tagged fields\n")
			gc.writeF("buff = binary.AppendUvarint(buff, uint64(%s))\n", numTaggedFieldsVarName)
			// Then write out the tagged fields themselves
			if len(taggedFields) > 0 {
				gc.write("// writing tagged fields\n")
				if err := genWriteFields(true, structName, taggedFields, gc); err != nil {
					return err
				}
			}
		}
		gc.closeVersionIf()
	}
	return nil
}

func genWriteFields(writeTagHeader bool, structName string, fields []MessageField, gc *genContext) error {
	var prevRange *versionRange
	for _, field := range fields {
		validRange, err := parseVersionRange(field.Versions)
		if err != nil {
			return err
		}
		// We don't want to start a new version check block if the version is the same as the previous one
		if prevRange == nil || !prevRange.equal(&validRange) {
			if prevRange != nil {
				// Version has changed, close previous block
				gc.closeVersionIf()
			}
			// Start a new version check
			if !gc.startVersionIf(validRange) {
				// Field not written at version
				gc.closeVersionIf()
				prevRange = nil
				continue
			}
		}
		prevRange = &validRange
		tagStartVarName := gc.varName("tagSizeStart")
		if writeTagHeader && field.TaggedVersions != "" {
			taggedVersionRange, err := parseVersionRange(field.TaggedVersions)
			if err != nil {
				return err
			}
			gc.startVersionIf(taggedVersionRange)
			gc.write("// tag header\n")
			gc.writeF("buff = binary.AppendUvarint(buff, uint64(%d))\n", field.Tag)
			gc.write("buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))\n")
			gc.write("tagPos++\n")
			gc.writeF("var %s int\n", tagStartVarName)
			gc.write("if debug.SanityChecks {\n")
			gc.writeF("    %s = len(buff)\n", tagStartVarName)
			gc.write("}\n")
			gc.addImport("github.com/spirit-labs/tektite/debug")
		}
		gc.writeF("// writing %s.%s: %s\n", structName, field.Name, field.About)
		fields := field.Fields
		if fields == nil {
			common, ok := gc.commonStructs[field.componentType()]
			if ok {
				fields = common.Fields
			}
		}
		if field.isArray() {
			if fields != nil {
				// array nested struct
				if err := genWriteFlexField(gc, &field, fmt.Sprintf("%s.%s", structName, field.Name)); err != nil {
					return err
				}
				varName := varName(field.Name)
				gc.writeF("for _, %s := range %s.%s {\n", varName, structName, field.Name)
				gc.incIndent()
				if err := genWriteForFields(varName, fields, gc); err != nil {
					return err
				}
				gc.decIndent()
				gc.write("}\n")
			} else {
				// can be []int32, []string, etc
				// write the length
				if err := genWriteFlexField(gc, &field, fmt.Sprintf("%s.%s", structName, field.Name)); err != nil {
					return err
				}
				varName := varName(field.Name)
				gc.writeF("for _, %s := range %s.%s {\n", varName, structName, field.Name)
				gc.incIndent()
				fieldCpy := field
				fieldCpy.FieldType = fieldCpy.componentType() // remove the []
				if err := genWriteSimpleField(varName, fieldCpy, gc); err != nil {
					return err
				}
				gc.decIndent()
				gc.write("}\n")
			}
		} else {
			// Not an array
			if fields != nil {
				// Non array nested struct
				gc.write("{\n")
				gc.incIndent()
				varName := fmt.Sprintf("%s.%s", structName, field.Name)
				if err := genWriteForFields(varName, fields, gc); err != nil {
					return err
				}
				gc.decIndent()
				gc.write("}\n")
			} else {
				if err := genWriteSimpleField(fmt.Sprintf("%s.%s", structName, field.Name), field, gc); err != nil {
					return err
				}
			}
		}
		if writeTagHeader && field.TaggedVersions != "" {
			gc.writeF("if debug.SanityChecks && len(buff) - %s != tagSizes[tagPos - 1] {\n", tagStartVarName)
			gc.writeF("    panic(fmt.Sprintf(\"incorrect calculated tag size for tag %s\", %d))\n", "%d", field.Tag)
			gc.write("}\n")
			gc.addImport("fmt")
			gc.closeVersionIf()
		}
	}
	if prevRange != nil {
		gc.closeVersionIf()
	}
	return nil
}

func genWriteSimpleField(receiverName string, field MessageField, gc *genContext) error {
	switch field.FieldType {
	case "bool":
		gc.writeF("if %s {\n", receiverName)
		gc.write("    buff = append(buff, 1)\n")
		gc.write("} else {\n")
		gc.write("    buff = append(buff, 0)\n")
		gc.write("}\n")
	case "int8":
		gc.writeF("buff = append(buff, byte(%s))\n", receiverName)
	case "int16":
		gc.writeF("buff = binary.BigEndian.AppendUint16(buff, uint16(%s))\n", receiverName)
	case "uint16":
		gc.writeF("buff = binary.BigEndian.AppendUint16(buff, %s)\n", receiverName)
	case "int32":
		gc.writeF("buff = binary.BigEndian.AppendUint32(buff, uint32(%s))\n", receiverName)
	case "uint32":
		gc.writeF("buff = binary.BigEndian.AppendUint32(buff, %s)\n", receiverName)
	case "int64":
		gc.writeF("buff = binary.BigEndian.AppendUint64(buff, uint64(%s))\n", receiverName)
	case "float64":
		gc.writeF("buff = binary.BigEndian.AppendUint64(buff, math.Float64bits(%s))\n", receiverName)
		gc.addImport("math")
	case "string":
		if err := genWriteFlexField(gc, &field, receiverName); err != nil {
			return err
		}
		gc.writeF("if %s != nil {\n", receiverName)
		gc.writeF("    buff = append(buff, *%s...)\n", receiverName)
		gc.write("}\n")
	case "uuid":
		gc.writeF("if %s != nil {\n", receiverName)
		gc.writeF("    buff = append(buff, %s...)\n", receiverName)
		gc.write("}\n")
	case "bytes":
		if err := genWriteFlexField(gc, &field, receiverName); err != nil {
			return err
		}
		gc.writeF("if %s != nil {\n", receiverName)
		gc.writeF("    buff = append(buff, %s...)\n", receiverName)
		gc.write("}\n")
	case "records":
		if err := genWriteFlexField(gc, &field, receiverName); err != nil {
			return err
		}
		gc.writeF("for _, rec := range %s {\n", receiverName)
		gc.write("    buff = append(buff, rec...)\n")
		gc.write("}\n")
	default:
		return errors.Errorf("unexpected type %s", field.FieldType)
	}
	return nil
}

func genWriteFlexField(gc *genContext, field *MessageField, structName string) error {
	ptr := ""
	if field.FieldType == "string" {
		ptr = "*"
	}
	flexRange, err := gc.flexibleRangeForField(field)
	if err != nil {
		return err
	}
	if flexRange != nil {
		if field.NullableVersions == "" {
			if gc.startVersionIf(*flexRange) {
				genWriteFlexibleNotNullable(gc, structName, field, ptr)
			}
			if gc.startVersionElse() {
				genWriteNotFlexibleNotNullable(gc, field, structName, ptr)
			}
			gc.closeVersionIf()
		} else {
			nullableRange, err := parseVersionRange(field.NullableVersions)
			if err != nil {
				return err
			}
			if gc.startVersionIf(*flexRange) {
				if gc.startVersionIf(nullableRange) {
					genWriteFlexibleNullable(gc, structName, field, ptr)
				}
				if gc.startVersionElse() {
					genWriteFlexibleNotNullable(gc, structName, field, ptr)
				}
				gc.closeVersionIf()
			}
			if gc.startVersionElse() {
				if gc.startVersionIf(nullableRange) {
					genWriteNotFlexibleNullable(gc, field, structName, ptr)
				}
				if gc.startVersionElse() {
					genWriteNotFlexibleNotNullable(gc, field, structName, ptr)
				}
				gc.closeVersionIf()
			}
			gc.closeVersionIf()
		}
	} else {
		if field.NullableVersions == "" {
			genWriteNotFlexibleNotNullable(gc, field, structName, ptr)
		} else {
			nullableRange, err := parseVersionRange(field.NullableVersions)
			if err != nil {
				return err
			}
			if gc.startVersionIf(nullableRange) {
				genWriteNotFlexibleNullable(gc, field, structName, ptr)
			}
			if gc.startVersionElse() {
				genWriteNotFlexibleNotNullable(gc, field, structName, ptr)
			}
			gc.closeVersionIf()
		}
	}
	return nil
}

func genWriteFlexibleNotNullable(gc *genContext, receiverName string, field *MessageField, ptr string) {
	gc.write("// flexible and not nullable\n")
	genWriteFlexLength(gc, receiverName, field, ptr)
}

func genWriteFlexibleNullable(gc *genContext, receiverName string, field *MessageField, ptr string) {
	gc.write("// flexible and nullable\n")
	gc.writeF("if %s == nil {\n", receiverName)
	gc.write("    // null\n")
	gc.write("    buff = append(buff, 0)\n")
	gc.write("} else {\n")
	gc.write("    // not null\n")
	gc.incIndent()
	genWriteFlexLength(gc, receiverName, field, ptr)
	gc.decIndent()
	gc.write("}\n")
}

func genWriteFlexLength(gc *genContext, receiverName string, field *MessageField, ptr string) {
	if field.FieldType == "records" {
		recordsTotSizeVarName := gc.varName("recordsTotSize")
		gc.writeF("%s := 0\n", recordsTotSizeVarName)
		gc.writeF("for _, rec := range %s {\n", receiverName)
		gc.writeF("    %s += len(rec)\n", recordsTotSizeVarName)
		gc.write("}\n")
		gc.writeF("buff = binary.AppendUvarint(buff, uint64(%s + 1))\n", recordsTotSizeVarName)
	} else {
		gc.writeF("buff = binary.AppendUvarint(buff, uint64(len(%s%s) + 1))\n", ptr, receiverName)
	}
}

func genWriteNotFlexibleNotNullable(gc *genContext, field *MessageField, receiverName string, ptr string) {
	gc.write("// non flexible and non nullable\n")
	intSize := "int32"
	if field.FieldType == "string" {
		intSize = "int16"
	}
	genWriteNonFlexLength(gc, receiverName, field, ptr, intSize)
}

func genWriteNotFlexibleNullable(gc *genContext, field *MessageField, receiverName string, ptr string) {
	intSize := "int32"
	minusOne := 4294967295
	if field.FieldType == "string" {
		intSize = "int16"
		minusOne = 65535
	}
	gc.write("// non flexible and nullable\n")
	gc.writeF("if %s == nil {\n", receiverName)
	gc.write("    // null\n")
	gc.writeF("    buff = binary.BigEndian.AppendU%s(buff, %d)\n", intSize, minusOne) // -1
	gc.write("} else {\n")
	gc.write("    // not null\n")
	gc.incIndent()
	genWriteNonFlexLength(gc, receiverName, field, ptr, intSize)
	gc.decIndent()
	gc.write("}\n")
}

func genWriteNonFlexLength(gc *genContext, receiverName string, field *MessageField, ptr string, intSize string) {
	if field.FieldType == "records" {
		recordsTotSizeVarName := gc.varName("recordsTotSize")
		gc.writeF("%s := 0\n", recordsTotSizeVarName)
		gc.writeF("for _, rec := range %s {\n", receiverName)
		gc.writeF("    %s += len(rec)\n", recordsTotSizeVarName)
		gc.write("}\n")
		gc.writeF("buff = binary.BigEndian.AppendU%s(buff, u%s(%s))\n", intSize, intSize, recordsTotSizeVarName)
	} else {
		gc.writeF("buff = binary.BigEndian.AppendU%s(buff, u%s(len(%s%s)))\n", intSize, intSize, ptr, receiverName)
	}
}

func genCalcSize(writingTagged bool, structName string, fields []MessageField, gc *genContext) error {
	numTaggedFieldsVarName := gc.varName("numTaggedFields")
	if gc.flexibleRange != nil {
		hasTaggedFields := false
		for _, field := range fields {
			if field.TaggedVersions != "" {
				hasTaggedFields = true
				break
			}
		}
		gc.writeF("%s:= 0\n", numTaggedFieldsVarName)
		gc.writeF("%s += 0\n", numTaggedFieldsVarName) // Prevent unused variable error
		if hasTaggedFields {
			gc.write("taggedFieldStart := 0\n")
			gc.write("taggedFieldSize := 0\n")
		}
	}
	var prevRange *versionRange
	for _, field := range fields {
		validRange, err := parseVersionRange(field.Versions)
		if err != nil {
			return err
		}

		// We don't want to start a new version check block if the version is the same as the previous one
		if prevRange == nil || !prevRange.equal(&validRange) {
			if prevRange != nil {
				// Version has changed, close previous block
				gc.closeVersionIf()
			}
			// Start a new version check
			if !gc.startVersionIf(validRange) {
				// If does not need to be written out as is never true
				gc.closeVersionIf()
				prevRange = nil
				continue
			}
		}
		prevRange = &validRange
		gc.writeF("// size for %s.%s: %s\n", structName, field.Name, field.About)
		if field.TaggedVersions != "" {
			taggedVersionRange, err := parseVersionRange(field.TaggedVersions)
			if err != nil {
				return err
			}
			if gc.startVersionIf(taggedVersionRange) {
				gc.writeF("%s++\n", numTaggedFieldsVarName)
				gc.write("taggedFieldStart = size\n")
			}
		}
		fields := field.Fields
		if fields == nil {
			common, ok := gc.commonStructs[field.componentType()]
			if ok {
				fields = common.Fields
			}
		}
		if field.isArray() {
			if fields != nil {
				// array of structs
				if err := genCalcSizeFlexibleField(gc, &field, fmt.Sprintf("%s.%s", structName, field.Name)); err != nil {
					return err
				}
				varName := varName(field.Name)
				gc.writeF("for _, %s := range %s.%s {\n", varName, structName, field.Name)
				gc.incIndent()
				// This is a hack to make sure the loop variable is never an unused variable
				// which would cause it to fail to compile - it always evaluates to zero so should have close to
				// zero performance overhead
				gc.addImport("unsafe")
				gc.writeF("size += 0 * int(unsafe.Sizeof(%s)) // hack to make sure loop variable is always used\n", varName)
				if err := genCalcForFields(varName, fields, gc); err != nil {
					return err
				}
				gc.decIndent()
				gc.write("}\n")
			} else {
				// can be []int32, []string, etc
				// write the length
				if err := genCalcSizeFlexibleField(gc, &field, fmt.Sprintf("%s.%s", structName, field.Name)); err != nil {
					return err
				}
				//varName := fmt.Sprintf("%s_%s", structName, field.Name)
				varName := varName(field.Name)
				gc.writeF("for _, %s := range %s.%s {\n", varName, structName, field.Name)
				gc.incIndent()
				gc.addImport("unsafe")
				gc.writeF("size += 0 * int(unsafe.Sizeof(%s)) // hack to make sure loop variable is always used\n", varName)
				fieldCpy := field
				fieldCpy.FieldType = fieldCpy.componentType() // remove the []
				if err := genCalcSizeSimpleField(varName, fieldCpy, gc); err != nil {
					return err
				}
				gc.decIndent()
				gc.write("}\n")
			}
		} else {
			if fields != nil {
				// Non array nested struct
				varName := fmt.Sprintf("%s.%s", structName, field.Name)
				gc.write("{\n")
				gc.incIndent()
				if err := genCalcForFields(varName, fields, gc); err != nil {
					return err
				}
				gc.decIndent()
				gc.write("}\n")
			} else {
				if err := genCalcSizeSimpleField(fmt.Sprintf("%s.%s", structName, field.Name), field, gc); err != nil {
					return err
				}
			}
		}
		if field.TaggedVersions != "" {
			gc.write("taggedFieldSize = size - taggedFieldStart\n")
			gc.write("tagSizes = append(tagSizes, taggedFieldSize)\n")
			gc.write("// size = <tag id contrib> + <field size>\n")
			gc.writeF("size += sizeofUvarint(%d) + sizeofUvarint(taggedFieldSize)\n", field.Tag)
			gc.closeVersionIf()
		}
	}
	if prevRange != nil {
		gc.closeVersionIf()
	}
	if writingTagged && gc.flexibleRange != nil {
		if gc.startVersionIf(*gc.flexibleRange) {
			gc.write("// writing size of num tagged fields field\n")
			gc.writeF("size += sizeofUvarint(%s)\n", numTaggedFieldsVarName)
		}
		gc.closeVersionIf()
	}
	return nil
}

func genCalcSizeSimpleField(receiverName string, field MessageField, gc *genContext) error {
	switch field.FieldType {
	case "bool":
		gc.write("size += 1\n")
	case "int8":
		gc.write("size += 1\n")
	case "int16":
		gc.write("size += 2\n")
	case "uint16":
		gc.write("size += 2\n")
	case "int32":
		gc.write("size += 4\n")
	case "uint32":
		gc.write("size += 4\n")
	case "int64":
		gc.write("size += 8\n")
	case "float64":
		gc.write("size += 8\n")
	case "string":
		if err := genCalcSizeFlexibleField(gc, &field, receiverName); err != nil {
			return err
		}
		gc.writeF("if %s != nil {\n", receiverName)
		gc.writeF("    size += len(*%s)\n", receiverName)
		gc.write("}\n")
	case "uuid":
		gc.write("size += 16\n")
	case "bytes":
		if err := genCalcSizeFlexibleField(gc, &field, receiverName); err != nil {
			return err
		}
		gc.writeF("if %s != nil {\n", receiverName)
		gc.writeF("    size += len(%s)\n", receiverName)
		gc.write("}\n")
	case "records":
		if err := genCalcSizeFlexibleField(gc, &field, receiverName); err != nil {
			return err
		}
		gc.writeF("for _, rec := range %s {\n", receiverName)
		gc.write("    size += len(rec)\n")
		gc.write("}\n")
	default:
		return errors.Errorf("unexpected type %s", field.FieldType)
	}
	return nil
}

func genCalcSizeFlexibleField(gc *genContext, field *MessageField, receiverName string) error {
	ptr := ""
	if field.FieldType == "string" {
		ptr = "*"
	}
	flexRange, err := gc.flexibleRangeForField(field)
	if err != nil {
		return err
	}
	if flexRange != nil {
		if field.NullableVersions == "" {
			if gc.startVersionIf(*flexRange) {
				genCalcSizeFlexibleNotNullable(gc, receiverName, field, ptr)
			}
			if gc.startVersionElse() {
				genCalcSizeNotFlexibleNotNullable(gc, field)
			}
			gc.closeVersionIf()
		} else {
			nullableRange, err := parseVersionRange(field.NullableVersions)
			if err != nil {
				return err
			}
			if gc.startVersionIf(*flexRange) {
				if gc.startVersionIf(nullableRange) {
					genCalcSizeFlexibleNullable(gc, receiverName, field, ptr)
				}
				if gc.startVersionElse() {
					genCalcSizeFlexibleNotNullable(gc, receiverName, field, ptr)
				}
				gc.closeVersionIf()
			}
			if gc.startVersionElse() {
				if gc.startVersionIf(nullableRange) {
					genCalcSizeNotFlexibleNullable(gc, field)
				}
				if gc.startVersionElse() {
					genCalcSizeNotFlexibleNotNullable(gc, field)
				}
				gc.closeVersionIf()
			}
			gc.closeVersionIf()
		}
	} else {
		if field.NullableVersions == "" {
			genCalcSizeNotFlexibleNotNullable(gc, field)
		} else {
			nullableRange, err := parseVersionRange(field.NullableVersions)
			if err != nil {
				return err
			}
			if gc.startVersionIf(nullableRange) {
				genCalcSizeNotFlexibleNullable(gc, field)
			}
			if gc.startVersionElse() {
				genCalcSizeNotFlexibleNotNullable(gc, field)
			}
			gc.closeVersionIf()
		}
	}
	return nil
}

func genCalcSizeFlexibleNotNullable(gc *genContext, receiverName string, field *MessageField, ptr string) {
	gc.write("// flexible and not nullable\n")
	genCalcSizeIncrementForField(gc, receiverName, field, ptr)
}

func genCalcSizeIncrementForField(gc *genContext, receiverName string, field *MessageField, ptr string) {
	if field.FieldType == "records" {
		// For records we need to calculate the total size
		recordsTotSizeVarName := gc.varName("recordsTotSize")
		gc.writeF("%s := 0\n", recordsTotSizeVarName)
		gc.writeF("for _, rec := range %s {\n", receiverName)
		gc.writeF("    %s += len(rec)\n", recordsTotSizeVarName)
		gc.write("}\n")
		gc.writeF("size += sizeofUvarint(%s + 1)\n", recordsTotSizeVarName)
	} else {
		gc.writeF("size += sizeofUvarint(len(%s%s) + 1)\n", ptr, receiverName)
	}
}

func genCalcSizeNotFlexibleNotNullable(gc *genContext, field *MessageField) {
	gc.write("// non flexible and non nullable\n")
	genCalcSizeNotFlexible(gc, field)
}

func genCalcSizeFlexibleNullable(gc *genContext, receiverName string, field *MessageField, ptr string) {
	gc.write("// flexible and nullable\n")
	gc.writeF("if %s == nil {\n", receiverName)
	gc.write("    // null\n")
	gc.write("    size += 1\n")
	gc.write("} else {\n")
	gc.write("    // not null\n")
	gc.incIndent()
	genCalcSizeIncrementForField(gc, receiverName, field, ptr)
	gc.decIndent()
	gc.write("}\n")
}

func genCalcSizeNotFlexibleNullable(gc *genContext, field *MessageField) {
	gc.write("// non flexible and nullable\n")
	genCalcSizeNotFlexible(gc, field)
}

func genCalcSizeNotFlexible(gc *genContext, field *MessageField) {
	if field.FieldType == "string" {
		gc.write("size += 2\n")
	} else {
		gc.write("size += 4\n")
	}
}

func genRangeIfStart(rng *versionRange, gc *genContext) {
	if rng.versionStart != 0 {
		if rng.versionEnd != -1 {
			gc.writeF("if version >= %d && version <= %d {\n", rng.versionStart, rng.versionEnd)
		} else {
			gc.writeF("if version >= %d {\n", rng.versionStart)
		}
	} else {
		if rng.versionEnd == -1 {
			panic("invalid range")
		}
		gc.writeF("if version <= %d {\n", rng.versionEnd)
	}
}

func parseType(s string) string {
	switch s {
	case "bool":
		return "bool"
	case "int8":
		return "int8"
	case "int16":
		return "int16"
	case "uint16":
		return "uint16"
	case "int32":
		return "int32"
	case "uint32":
		return "uint32"
	case "int64":
		return "int64"
	case "float64":
		return "float64"
	case "string":
		return "*string"
	case "uuid":
		return "[]byte"
	case "bytes":
		return "[]byte"
	case "records":
		return "[][]byte"
	default:
		return s
	}
}

func parseVersionRange(s string) (versionRange, error) {
	if strings.HasSuffix(s, "+") {
		start, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return versionRange{}, err
		}
		return versionRange{
			versionStart: start,
			versionEnd:   -1,
		}, nil
	}
	parts := strings.Split(s, "-")
	if len(parts) > 2 {
		return versionRange{}, fmt.Errorf("invalid versionRange: %s", s)
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return versionRange{}, err
	}
	end := start
	if len(parts) == 2 {
		end, err = strconv.Atoi(parts[1])
		if err != nil {
			return versionRange{}, err
		}
	}
	return versionRange{
		versionStart: start,
		versionEnd:   end,
	}, nil
}

type versionRange struct {
	versionStart int
	versionEnd   int
}

// containsRange returns true if the provided range is equal to or is a sub range of this range
func (v *versionRange) containsRange(rng *versionRange) bool {
	if rng.versionStart < v.versionStart {
		return false
	}
	if rng.versionEnd == -1 {
		return v.versionEnd == -1
	}
	return rng.versionEnd <= v.versionEnd
}

func (v *versionRange) intersects(rng *versionRange) bool {
	dontOverlapLeft := rng.versionEnd != -1 && rng.versionEnd < v.versionStart
	dontOverlapRight := v.versionEnd != -1 && rng.versionStart > v.versionEnd
	return !(dontOverlapLeft || dontOverlapRight)
}

func (v *versionRange) allRange() bool {
	return v.versionStart == 0 && v.versionEnd == -1
}

func (v *versionRange) containsVersion(version int) bool {
	return v.versionStart <= version && (v.versionEnd == -1 || version <= v.versionEnd)
}

func (v *versionRange) equal(vr *versionRange) bool {
	if vr == nil {
		return false
	}
	return v.versionStart == vr.versionStart && v.versionEnd == vr.versionEnd
}

type MessageSpec struct {
	ApiKey             int            `json:"apiKey"`
	MessageType        string         `json:"type"`
	Listeners          []string       `json:"listeners"`
	Name               string         `json:"name"`
	ValidVersions      string         `json:"validVersions"`
	DeprecatedVersions string         `json:"deprecatedVersions"`
	FlexibleVersions   string         `json:"flexibleVersions"`
	Fields             []MessageField `json:"fields"`
	CommonStructs      []MessageField `json:"commonStructs"`
}

type MessageField struct {
	Name             string         `json:"name"`
	FieldType        string         `json:"type"`
	Versions         string         `json:"versions"`
	NullableVersions string         `json:"nullableVersions"`
	FlexibleVersions string         `json:"flexibleVersions"`
	EntityType       string         `json:"entityType"`
	About            string         `json:"about"`
	TaggedVersions   string         `json:"taggedVersions"`
	Tag              int            `json:"tag"`
	Fields           []MessageField `json:"fields"`
}

func (f *MessageField) supportsNullable() bool {
	return f.FieldType == "string" || f.FieldType == "uuid" || f.FieldType == "bytes" || f.FieldType == "records"
}

func (f *MessageField) isArray() bool {
	return f.FieldType[:2] == "[]"
}

func (f *MessageField) componentType() string {
	if f.isArray() {
		return f.FieldType[2:]
	} else {
		return f.FieldType
	}
}

type genContext struct {
	sb            strings.Builder
	indent        int
	indentSpaces  string
	flexibleRange *versionRange
	versionIfs    []*versionIf
	imports       map[string]struct{}
	loopIndex     int
	varSeq        int
	commonStructs map[string]MessageField
	spec          MessageSpec
}

func (gc *genContext) addImport(importS string) {
	gc.imports[importS] = struct{}{}
}

func (gc *genContext) flexibleRangeForField(field *MessageField) (*versionRange, error) {
	if field.FlexibleVersions != "" {
		if field.FlexibleVersions == "none" {
			return nil, nil
		}
		rng, err := parseVersionRange(field.FlexibleVersions)
		if err != nil {
			return nil, err
		}
		return &rng, nil
	}
	return gc.flexibleRange, nil
}

func (gc *genContext) string() string {
	var sb strings.Builder
	sb.WriteString("// Package protocol - This is a generated file, please do not edit\n\n")
	sb.WriteString("package protocol\n\n")
	sb.WriteString("import \"encoding/binary\"\n")
	var imports []string
	for imp := range gc.imports {
		imports = append(imports, imp)
	}
	// sort them so we have deterministic order
	sort.Strings(imports)
	for _, imp := range imports {
		sb.WriteString(fmt.Sprintf("import \"%s\"\n", imp))
	}
	sb.WriteString("\n")
	sb.WriteString(gc.sb.String())
	return sb.String()
}

type versionIf struct {
	rng     versionRange
	printed bool
}

/*


We can either:

Write out the if with the else

Write out just the if

Write out just the else

So we figure out

1. Is the if always true?
2. Is the if always false?
3. Can it be either?

So we have 3 states: ALWAYS_TRUE, ALWAYS_FALSE, INDETERMINATE

So...

When calling startIf:

a. calculate states
b. if INDETERMINATE then output the "if ... {" line
c. if INDETERMINATE | ALWAYS_TRUE return true

When calling startElse:

a. calculate states
b. if INDETERMINATE then output the "else {" line
c. if INDETERMINATE | ALWAYS_FALSE return true


When calling closeIF:

a. calculate states
b. if INDETERMINATE then output the "}" line


How to calculate states

If the later range contains the previous written range, then the if is ALWAYS_TRUE
If the later range and the previous range have no intersection then it is ALWAYS_FALSE
Otherwise INDETERMINATE
*/

type ifStatus int

const (
	alwaysTrue = iota
	alwaysFalse
	indeterminate
)

// calcIfElseStatus calculates whether the latest if... else if always true, always false or indeterminate.
// If it's always true or always false we can avoid generating it and just output the contents of the if block
// or else block directly
func (gc *genContext) calcIfElseStatus() ifStatus {
	curr := gc.versionIfs[len(gc.versionIfs)-1].rng
	var lastPrinted *versionIf
	for i := len(gc.versionIfs) - 2; i >= 0; i-- {
		if gc.versionIfs[i].printed {
			lastPrinted = gc.versionIfs[i]
			break
		}
	}
	if (lastPrinted != nil && curr.containsRange(&lastPrinted.rng)) || curr.allRange() {
		// If the later range contains the previous range or its an all encompassing range (0-*) then it must always
		// be true
		return alwaysTrue
	}
	if lastPrinted != nil && !lastPrinted.rng.intersects(&curr) {
		// If there is no intersection between the later range and the previous range then it must be always false
		return alwaysFalse
	}
	return indeterminate
}

// startVersionIf returns true if the body of the if needs to be written out - i.e. can it possibly occur
func (gc *genContext) startVersionIf(rng versionRange) bool {
	vif := &versionIf{rng: rng}
	gc.versionIfs = append(gc.versionIfs, vif)
	status := gc.calcIfElseStatus()
	if status == indeterminate {
		// write out the if statement
		genRangeIfStart(&rng, gc)
		gc.incIndent()
		vif.printed = true
	}
	// the body of the if will be written if indeterminate or always true
	return status == indeterminate || status == alwaysTrue
}

func (gc *genContext) startVersionElse() bool {
	status := gc.calcIfElseStatus()
	if status == indeterminate {
		gc.decIndent()
		gc.write("} else {\n")
		gc.incIndent()
	}
	return status == indeterminate || status == alwaysFalse
}

func (gc *genContext) closeVersionIf() {
	if len(gc.versionIfs) == 0 {
		return
	}
	status := gc.calcIfElseStatus()
	if status == indeterminate {
		gc.decIndent()
		gc.write("}\n")
	}
	gc.versionIfs = gc.versionIfs[:len(gc.versionIfs)-1]
}

func (gc *genContext) write(s string) {
	str := fmt.Sprintf("%s%s", gc.indentSpaces, s)
	gc.sb.WriteString(str)
}

func (gc *genContext) writeF(s string, args ...any) {
	gc.write(fmt.Sprintf(s, args...))
}

func (gc *genContext) incIndent() {
	gc.indent += 4
	gc.updateIndentSpaces()
}

func (gc *genContext) decIndent() {
	gc.indent -= 4
	gc.updateIndentSpaces()
}

func (gc *genContext) incLoopIndex() {
	gc.loopIndex++
}

func (gc *genContext) decLoopIndex() {
	gc.loopIndex--
}

func (gc *genContext) updateIndentSpaces() {
	bytes := make([]byte, gc.indent)
	for i := 0; i < gc.indent; i++ {
		bytes[i] = ' '
	}
	gc.indentSpaces = string(bytes)
}

func (gc *genContext) varName(prefix string) string {
	varName := fmt.Sprintf("%s%d", prefix, gc.varSeq)
	gc.varSeq++
	return varName
}
