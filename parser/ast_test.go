package parser

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestFailedToParseStreamName(t *testing.T) {
	input := "5h := (kafka in partitions 10)"
	expectedMsg := `expected identifier but found '5h' (line 1 column 1):
5h := (kafka in partitions 10)
^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "123 := (kafka in partitions 10)"
	expectedMsg = `expected identifier but found '123' (line 1 column 1):
123 := (kafka in partitions 10)
^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseStreamAssignment(t *testing.T) {
	input := "my_stream foo (kafka in partitions 10)"
	expectedMsg := `expected ':=' but found 'foo' (line 1 column 11):
my_stream foo (kafka in partitions 10)
          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream"
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseOperatorOpenParens(t *testing.T) {
	input := "my_stream := kafka in partitions 10"
	expectedMsg := `expected '->' but found 'in' (line 1 column 20):
my_stream := kafka in partitions 10
                   ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseOperatorName(t *testing.T) {
	input := "my_stream := (wibble foo=24h)"
	expectedMsg := `expected one of: 'aggregate', 'backfill', 'bridge', 'filter', 'join', 'kafka', 'partition', 'producer', 'project', 'store', 'topic', 'union' (line 1 column 15):
my_stream := (wibble foo=24h)
              ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := ("
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseMissingOperatorCloseParens(t *testing.T) {
	input := `my_stream := (bridge from test_topic partitions = 20 props = ("fakeKafkaID" = "0") -> (store stream)`
	expectedMsg := `expected identifier or closing ')' but found '->' (line 1 column 84):
my_stream := (bridge from test_topic partitions = 20 props = ("fakeKafkaID" = "0") -> (store stream)
                                                                                   ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseContinuation(t *testing.T) {
	input := "my_stream := parent_stream -> (store stream)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&ContinuationDesc{ParentStreamName: "parent_stream"},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseBridgeFromMandatoryArgs(t *testing.T) {
	input := `my_stream := (bridge from my_topic partitions = 23)`
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeFromDesc{
				TopicName:  "my_topic",
				Partitions: 23,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseBridgeFromAllArgs(t *testing.T) {
	input := `my_stream :=
(
	bridge from my_topic
	partitions = 23
	poll_timeout = 1500ms
	max_poll_messages = 2323
	watermark_type = processing_time
	watermark_lateness = 5s
	watermark_idle_timeout = 30s
	props = ("prop1" = "val1" "prop2" = "val2" "prop3" = "val3")
)`
	pollTimeout := 1500 * time.Millisecond
	maxPollMessages := 2323
	watermarkType := "processing_time"
	watermarkLateness := 5 * time.Second
	watermarkIdleTimeout := 30 * time.Second
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeFromDesc{
				TopicName:            "my_topic",
				Partitions:           23,
				PollTimeout:          &pollTimeout,
				MaxPollMessages:      &maxPollMessages,
				WatermarkType:        &watermarkType,
				WatermarkLateness:    &watermarkLateness,
				WatermarkIdleTimeout: &watermarkIdleTimeout,
				Props: map[string]string{
					"prop1": "val1",
					"prop2": "val2",
					"prop3": "val3",
				},
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseBridgeFromAllArgsNoEquals(t *testing.T) {
	input := `my_stream :=
(
	bridge from my_topic
	partitions 23
	poll_timeout 1500ms
	max_poll_messages 2323
	watermark_type processing_time
	watermark_lateness 5s
	watermark_idle_timeout 30s
	props ("prop1" "val1" "prop2" "val2" "prop3" "val3")
)`
	pollTimeout := 1500 * time.Millisecond
	maxPollMessages := 2323
	watermarkType := "processing_time"
	watermarkLateness := 5 * time.Second
	watermarkIdleTimeout := 30 * time.Second
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeFromDesc{
				TopicName:            "my_topic",
				Partitions:           23,
				PollTimeout:          &pollTimeout,
				MaxPollMessages:      &maxPollMessages,
				WatermarkType:        &watermarkType,
				WatermarkLateness:    &watermarkLateness,
				WatermarkIdleTimeout: &watermarkIdleTimeout,
				Props: map[string]string{
					"prop1": "val1",
					"prop2": "val2",
					"prop3": "val3",
				},
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseBridgeFromEmptyProps(t *testing.T) {
	input := `my_stream :=
(
	bridge from my_topic
	partitions 23
	props ()
)`
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeFromDesc{
				TopicName:  "my_topic",
				Partitions: 23,
				Props:      map[string]string{},
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseBridgeFromEmpty(t *testing.T) {
	input := `my_stream := (bridge from`
	expectedMsg := `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseBridgeFromTopicName(t *testing.T) {
	input := `my_stream := (bridge from 5h)`
	expectedMsg := `expected identifier but found '5h' (line 1 column 27):
my_stream := (bridge from 5h)
                          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from 23)`
	expectedMsg = `expected identifier but found '23' (line 1 column 27):
my_stream := (bridge from 23)
                          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseBridgeFromPartitions(t *testing.T) {
	input := `my_stream := (bridge from some_topic)`
	expectedMsg := `expected 'partitions' but found ')' (line 1 column 37):
my_stream := (bridge from some_topic)
                                    ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions)`
	expectedMsg = `expected '=' or integer but found ')' (line 1 column 48):
my_stream := (bridge from some_topic partitions)
                                               ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions=)`
	expectedMsg = `expected integer but found ')' (line 1 column 49):
my_stream := (bridge from some_topic partitions=)
                                                ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions = foo)`
	expectedMsg = `expected integer but found 'foo' (line 1 column 51):
my_stream := (bridge from some_topic partitions = foo)
                                                  ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions foo)`
	expectedMsg = `expected '=' or integer but found 'foo' (line 1 column 49):
my_stream := (bridge from some_topic partitions foo)
                                                ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions = 24h)`
	expectedMsg = `expected integer but found '24h' (line 1 column 51):
my_stream := (bridge from some_topic partitions = 24h)
                                                  ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseBridgeFromProps(t *testing.T) {
	input := `my_stream := (bridge from some_topic partitions 23 props)`
	expectedMsg := `expected '=' or '(' but found ')' (line 1 column 57):
my_stream := (bridge from some_topic partitions 23 props)
                                                        ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions 23 props = )`
	expectedMsg = `expected '(' but found ')' (line 1 column 60):
my_stream := (bridge from some_topic partitions 23 props = )
                                                           ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions 23 props = (foo))`
	expectedMsg = `expected string literal but found 'foo' (line 1 column 61):
my_stream := (bridge from some_topic partitions 23 props = (foo))
                                                            ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions 23 props = ("foo"))`
	expectedMsg = `expected '=' or string literal but found ')' (line 1 column 66):
my_stream := (bridge from some_topic partitions 23 props = ("foo"))
                                                                 ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions 23 props = ("foo"=))`
	expectedMsg = `expected string literal but found ')' (line 1 column 67):
my_stream := (bridge from some_topic partitions 23 props = ("foo"=))
                                                                  ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions 23 props = ("foo"=bar))`
	expectedMsg = `expected string literal but found 'bar' (line 1 column 67):
my_stream := (bridge from some_topic partitions 23 props = ("foo"=bar))
                                                                  ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions 23 props = ("foo"=5h))`
	expectedMsg = `expected string literal but found '5h' (line 1 column 67):
my_stream := (bridge from some_topic partitions 23 props = ("foo"=5h))
                                                                  ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions 23 props = ("foo"="bar", "quux"="wibble"))`
	expectedMsg = `expected string literal but found ',' (line 1 column 72):
my_stream := (bridge from some_topic partitions 23 props = ("foo"="bar", "quux"="wibble"))
                                                                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseBridgeFromDuplicateArgs(t *testing.T) {
	input := `my_stream := (bridge from some_topic partitions 23 partitions 26)`
	expectedMsg := `argument 'partitions' is duplicated (line 1 column 52):
my_stream := (bridge from some_topic partitions 23 partitions 26)
                                                   ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge from some_topic partitions 23 watermark_lateness = 5m watermark_lateness 10m)`
	expectedMsg = `argument 'watermark_lateness' is duplicated (line 1 column 76):
my_stream := (bridge from some_topic partitions 23 watermark_lateness = 5m watermark_lateness 10m)
                                                                           ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseBridgeFromUnknownArgs(t *testing.T) {
	input := `my_stream := (bridge from some_topic partitions 23 badgers 43)`
	expectedMsg := `unknown argument 'badgers' (line 1 column 52):
my_stream := (bridge from some_topic partitions 23 badgers 43)
                                                   ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseBridgeTo(t *testing.T) {
	input := `my_stream := (bridge to some_topic retention = 23h
initial_retry_delay = 11s max_retry_delay = 13s connect_timeout = 7s send_timeout = 9s
props = ("prop1" = "val1" "prop2" = "val2" "prop3" = "val3")`
	retention := 23 * time.Hour
	initialRetryDeay := 11 * time.Second
	maxRetrydelay := 13 * time.Second
	connectTimeout := 7 * time.Second
	sendTimeout := 9 * time.Second
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeToDesc{
				TopicName:         "some_topic",
				Retention:         &retention,
				InitialRetryDelay: &initialRetryDeay,
				MaxRetryDelay:     &maxRetrydelay,
				ConnectTimeout:    &connectTimeout,
				SendTimeout:       &sendTimeout,
				Props: map[string]string{
					"prop1": "val1",
					"prop2": "val2",
					"prop3": "val3",
				},
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseBridgeToNoProps(t *testing.T) {
	input := `my_stream := (bridge to some_topic)`
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeToDesc{
				TopicName: "some_topic",
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseBridgeToEmptyProps(t *testing.T) {
	input := `my_stream := (bridge to some_topic props=())`
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeToDesc{
				TopicName: "some_topic",
				Props:     map[string]string{},
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseBridgeTo(t *testing.T) {
	input := `my_stream := (bridge to)`
	expectedMsg := `expected identifier but found ')' (line 1 column 24):
my_stream := (bridge to)
                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge to`
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge to some_topic)`
	expectedMsg = `expected 'partitions' but found ')' (line 1 column 35):
my_stream := (bridge to some_topic)
                                  ^`

	input = `my_stream := (bridge to some_topic props = (prop1 = val1))`
	expectedMsg = `expected string literal but found 'prop1' (line 1 column 45):
my_stream := (bridge to some_topic props = (prop1 = val1))
                                            ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (bridge to some_topic props = ("prop1" = val1))`
	expectedMsg = `expected string literal but found 'val1' (line 1 column 55):
my_stream := (bridge to some_topic props = ("prop1" = val1))
                                                      ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func testParseCreateStream(t *testing.T, input string, expected CreateStreamDesc) {
	cs := NewCreateStreamDesc()
	err := NewParser(nil).Parse(input, cs)
	require.NoError(t, err)
	// need to blank the BaseDesc fields
	cs.clearTokenState()
	require.Equal(t, &expected, cs)
}

func testFailedToParseCreateStream(t *testing.T, input string, expectedMsg string) {
	cs := NewCreateStreamDesc()
	err := NewParser(nil).Parse(input, cs)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.ParseError))
	require.Equal(t, expectedMsg, err.Error())
}

func TestParseProject(t *testing.T) {

	testParseProject(t, "f1", &IdentifierExprDesc{IdentifierName: "f1"})
	testParseProject(t, "f1, f2, f3, f4",
		&IdentifierExprDesc{IdentifierName: "f1"},
		&IdentifierExprDesc{IdentifierName: "f2"},
		&IdentifierExprDesc{IdentifierName: "f3"},
		&IdentifierExprDesc{IdentifierName: "f4"})

	testParseProject(t, "f1 + f2, f3 + f4",
		&BinaryOperatorExprDesc{
			Left:  &IdentifierExprDesc{IdentifierName: "f1"},
			Right: &IdentifierExprDesc{IdentifierName: "f2"},
			Op:    "+",
		},
		&BinaryOperatorExprDesc{
			Left:  &IdentifierExprDesc{IdentifierName: "f3"},
			Right: &IdentifierExprDesc{IdentifierName: "f4"},
			Op:    "+",
		},
	)

	testParseProject(t, "f1 as col1, f2 == 1004 as col2",
		&BinaryOperatorExprDesc{
			Left:  &IdentifierExprDesc{IdentifierName: "f1"},
			Right: &IdentifierExprDesc{IdentifierName: "col1"},
			Op:    "as",
		},
		&BinaryOperatorExprDesc{
			Left: &BinaryOperatorExprDesc{
				Left:  &IdentifierExprDesc{IdentifierName: "f2"},
				Right: &IntegerConstExprDesc{Value: 1004},
				Op:    "==",
			},
			Right: &IdentifierExprDesc{IdentifierName: "col2"},
			Op:    "as",
		},
	)

	testParseProject(t, "to_string(f1),to_int(f2)",
		&FunctionExprDesc{
			FunctionName: "to_string",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		},
		&FunctionExprDesc{
			FunctionName: "to_int",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f2"},
			},
		},
	)

	testParseProject(t, "rtrim(f1,f2),ltrim(f3,f4)",
		&FunctionExprDesc{
			FunctionName: "rtrim",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IdentifierExprDesc{IdentifierName: "f2"},
			},
		},
		&FunctionExprDesc{
			FunctionName: "ltrim",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f3"},
				&IdentifierExprDesc{IdentifierName: "f4"},
			},
		},
	)

	testParseProject(t, "rtrim(f1,ltrim(f2, f3)),ltrim(rtrim(f3, f4),f4)",
		&FunctionExprDesc{
			FunctionName: "rtrim",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&FunctionExprDesc{
					FunctionName: "ltrim",
					ArgExprs: []ExprDesc{
						&IdentifierExprDesc{IdentifierName: "f2"},
						&IdentifierExprDesc{IdentifierName: "f3"},
					},
				},
			},
		},
		&FunctionExprDesc{
			FunctionName: "ltrim",
			ArgExprs: []ExprDesc{
				&FunctionExprDesc{
					FunctionName: "rtrim",
					ArgExprs: []ExprDesc{
						&IdentifierExprDesc{IdentifierName: "f3"},
						&IdentifierExprDesc{IdentifierName: "f4"},
					},
				},
				&IdentifierExprDesc{IdentifierName: "f4"},
			},
		},
	)

	testParseProject(t, "json_int(f1,f2) + f10,json_float(f3,f4) + f11",
		&BinaryOperatorExprDesc{
			Left: &FunctionExprDesc{
				FunctionName: "json_int",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f1"},
					&IdentifierExprDesc{IdentifierName: "f2"},
				},
			},
			Right: &IdentifierExprDesc{IdentifierName: "f10"},
			Op:    "+",
		},
		&BinaryOperatorExprDesc{
			Left: &FunctionExprDesc{
				FunctionName: "json_float",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f3"},
					&IdentifierExprDesc{IdentifierName: "f4"},
				},
			},
			Right: &IdentifierExprDesc{IdentifierName: "f11"},
			Op:    "+",
		},
	)

	testParseProject(t, "to_int(f1) + to_int(f2),to_float(f3) + to_float(f4)",
		&BinaryOperatorExprDesc{
			Left: &FunctionExprDesc{
				FunctionName: "to_int",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f1"},
				},
			},
			Right: &FunctionExprDesc{
				FunctionName: "to_int",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f2"},
				},
			},
			Op: "+",
		},
		&BinaryOperatorExprDesc{
			Left: &FunctionExprDesc{
				FunctionName: "to_float",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f3"},
				},
			},
			Right: &FunctionExprDesc{
				FunctionName: "to_float",
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f4"},
				},
			},
			Op: "+",
		},
	)

	testParseProject(t, `ltrim("hello",f1),rtrim("hello",f2)`,
		&FunctionExprDesc{
			FunctionName: "ltrim",
			ArgExprs: []ExprDesc{
				&StringConstExprDesc{Value: "hello"},
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		},
		&FunctionExprDesc{
			FunctionName: "rtrim",
			ArgExprs: []ExprDesc{
				&StringConstExprDesc{Value: "hello"},
				&IdentifierExprDesc{IdentifierName: "f2"},
			},
		},
	)

	testParseProject(t, `f1, f2, f3, to_int(f4), f5`,
		&IdentifierExprDesc{IdentifierName: "f1"},
		&IdentifierExprDesc{IdentifierName: "f2"},
		&IdentifierExprDesc{IdentifierName: "f3"},
		&FunctionExprDesc{
			FunctionName: "to_int",
			ArgExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f4"},
			},
		},
		&IdentifierExprDesc{IdentifierName: "f5"},
	)
}

func testParseProject(t *testing.T, exprStr string, expectedExprs ...ExprDesc) {
	input := fmt.Sprintf(`my_stream := (project %s)`, exprStr)
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&ProjectDesc{
				Expressions: expectedExprs,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseProject(t *testing.T) {
	input := `my_stream := (project f1, func1(f2 )`
	expectedMsg := `'func1' is not a known function (line 1 column 27):
my_stream := (project f1, func1(f2 )
                          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (project f1, to_string(f2 )`
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (project)`
	expectedMsg = `there must be at least one expression (line 1 column 22):
my_stream := (project)
                     ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (project`
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParsePartition(t *testing.T) {
	input := "my_stream := (partition by f1, f2 partitions 23)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&PartitionDesc{
				KeyExprs:   []string{"f1", "f2"},
				Partitions: 23,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (partition by f1, f2 partitions 23 mapping "my-mapping")`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&PartitionDesc{
				KeyExprs:   []string{"f1", "f2"},
				Partitions: 23,
				Mapping:    `"my-mapping"`,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (partition by f1, f2 partitions 23 mapping "my-mapping")`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&PartitionDesc{
				KeyExprs:   []string{"f1", "f2"},
				Partitions: 23,
				Mapping:    `"my-mapping"`,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParsePartitionWithEquals(t *testing.T) {
	input := "my_stream := (partition by f1, f2 partitions=23)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&PartitionDesc{
				KeyExprs:   []string{"f1", "f2"},
				Partitions: 23,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (partition by f1, f2 partitions=23 mapping="my-mapping")`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&PartitionDesc{
				KeyExprs:   []string{"f1", "f2"},
				Partitions: 23,
				Mapping:    `"my-mapping"`,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (partition by f1, f2 partitions 23 mapping "my-mapping")`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&PartitionDesc{
				KeyExprs:   []string{"f1", "f2"},
				Partitions: 23,
				Mapping:    `"my-mapping"`,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParsePartition(t *testing.T) {
	input := `my_stream := (partition)`
	expectedMsg := `expected 'by' but found ')' (line 1 column 24):
my_stream := (partition)
                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (partition`
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (partition by)`
	expectedMsg = `there must be at least one expression (line 1 column 27):
my_stream := (partition by)
                          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (partition by f1, to_lower(f2)`
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (partition by f1, f2)`
	expectedMsg = `expected 'partitions' but found ')' (line 1 column 34):
my_stream := (partition by f1, f2)
                                 ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (partition by f1, f2 partitions)`
	expectedMsg = `expected '=' or integer but found ')' (line 1 column 45):
my_stream := (partition by f1, f2 partitions)
                                            ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (partition by f1, f2 partitions = foo)`
	expectedMsg = `expected integer but found 'foo' (line 1 column 48):
my_stream := (partition by f1, f2 partitions = foo)
                                               ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = `my_stream := (partition by f1, f2 partitions = 23.23f)`
	expectedMsg = `expected integer but found '23.23f' (line 1 column 48):
my_stream := (partition by f1, f2 partitions = 23.23f)
                                               ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseAggregate(t *testing.T) {
	testParseAggregate(t, "sum(f1) by f2", []string{"sum(f1)"},
		[]ExprDesc{&FunctionExprDesc{
			FunctionName: "sum",
			Aggregate:    true,
			ArgExprs: []ExprDesc{&IdentifierExprDesc{
				IdentifierName: "f1",
			}},
		}}, []string{"f2"}, []ExprDesc{&IdentifierExprDesc{
			IdentifierName: "f2",
		}})

	testParseAggregate(t, "sum(f2) by f1", []string{"sum(f2)"},
		[]ExprDesc{&FunctionExprDesc{
			FunctionName: "sum",
			Aggregate:    true,
			ArgExprs: []ExprDesc{&IdentifierExprDesc{
				IdentifierName: "f2",
			}},
		}}, []string{"f1"}, []ExprDesc{&IdentifierExprDesc{
			IdentifierName: "f1",
		}})

	testParseAggregate(t, "sum(f3),count(f4) by f1,f2",
		[]string{"sum(f3)", "count(f4)"},
		[]ExprDesc{
			&FunctionExprDesc{
				FunctionName: "sum",
				Aggregate:    true,
				ArgExprs: []ExprDesc{&IdentifierExprDesc{
					IdentifierName: "f3",
				}},
			},
			&FunctionExprDesc{
				FunctionName: "count",
				Aggregate:    true,
				ArgExprs: []ExprDesc{&IdentifierExprDesc{
					IdentifierName: "f4",
				}},
			},
		},
		[]string{"f1", "f2"},
		[]ExprDesc{
			&IdentifierExprDesc{IdentifierName: "f1"},
			&IdentifierExprDesc{IdentifierName: "f2"},
		},
	)

	testParseAggregate(t, "sum(f1) by to_lower(f1)",
		[]string{"sum(f1)"},
		[]ExprDesc{
			&FunctionExprDesc{
				FunctionName: "sum",
				Aggregate:    true,
				ArgExprs: []ExprDesc{&IdentifierExprDesc{
					IdentifierName: "f1",
				}},
			},
		},
		[]string{"to_lower(f1)"},
		[]ExprDesc{
			&FunctionExprDesc{
				FunctionName: "to_lower",
				Aggregate:    false,
				ArgExprs: []ExprDesc{&IdentifierExprDesc{
					IdentifierName: "f1",
				}},
			},
		},
	)

	testParseAggregate(t, "sum(f3) by f1 + f2",
		[]string{"sum(f3)"},
		[]ExprDesc{
			&FunctionExprDesc{
				FunctionName: "sum",
				Aggregate:    true,
				ArgExprs: []ExprDesc{&IdentifierExprDesc{
					IdentifierName: "f3",
				}},
			},
		},
		[]string{"f1+f2"},
		[]ExprDesc{
			&BinaryOperatorExprDesc{
				Left: &IdentifierExprDesc{
					IdentifierName: "f1",
				},
				Right: &IdentifierExprDesc{
					IdentifierName: "f2",
				},
				Op: "+",
			},
		},
	)

	testParseAggregate(t, "sum(f2+f3) by f1",
		[]string{"sum(f2+f3)"},
		[]ExprDesc{
			&FunctionExprDesc{
				FunctionName: "sum",
				Aggregate:    true,
				ArgExprs: []ExprDesc{
					&BinaryOperatorExprDesc{
						Left: &IdentifierExprDesc{
							IdentifierName: "f2",
						},
						Right: &IdentifierExprDesc{
							IdentifierName: "f3",
						},
						Op: "+",
					},
				},
			},
		},
		[]string{"f1"},
		[]ExprDesc{
			&IdentifierExprDesc{
				IdentifierName: "f1",
			},
		},
	)

	testParseAggregate(t, "sum(f2+to_lower(f3)) by f1",
		[]string{"sum(f2+to_lower(f3))"},
		[]ExprDesc{
			&FunctionExprDesc{
				FunctionName: "sum",
				Aggregate:    true,
				ArgExprs: []ExprDesc{
					&BinaryOperatorExprDesc{
						Left: &IdentifierExprDesc{
							IdentifierName: "f2",
						},
						Right: &FunctionExprDesc{
							FunctionName: "to_lower",
							Aggregate:    false,
							ArgExprs: []ExprDesc{&IdentifierExprDesc{
								IdentifierName: "f3",
							}},
						},
						Op: "+",
					},
				},
			},
		},
		[]string{"f1"},
		[]ExprDesc{
			&IdentifierExprDesc{
				IdentifierName: "f1",
			},
		},
	)

	testParseAggregate(t, "sum(f3), count(f2)",
		[]string{"sum(f3)", "count(f2)"},
		[]ExprDesc{
			&FunctionExprDesc{
				FunctionName: "sum",
				Aggregate:    true,
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{
						IdentifierName: "f3",
					},
				},
			},
			&FunctionExprDesc{
				FunctionName: "count",
				Aggregate:    true,
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{
						IdentifierName: "f2",
					},
				},
			},
		},
		nil,
		nil,
	)

	testParseAggregate(t, "count(f2)",
		[]string{"count(f2)"},
		[]ExprDesc{
			&FunctionExprDesc{
				FunctionName: "count",
				Aggregate:    true,
				ArgExprs: []ExprDesc{
					&IdentifierExprDesc{
						IdentifierName: "f2",
					},
				},
			},
		},
		nil,
		nil,
	)
}

func testParseAggregate(t *testing.T, colsStr string, expectedAggStrs []string, expectedAggExprs []ExprDesc, expectedKeyStrs []string,
	expectedKeyExprs []ExprDesc) {
	input := fmt.Sprintf("my_stream := (aggregate %s)", colsStr)
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&AggregateDesc{
				AggregateExprStrings: expectedAggStrs,
				AggregateExprs:       expectedAggExprs,
				KeyExprsStrings:      expectedKeyStrs,
				KeyExprs:             expectedKeyExprs,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseAggregateWithArgs(t *testing.T) {
	input := "my_stream := (aggregate sum(f1), count(f2) by f3, to_lower(f4) size 5m hop 1m lateness 30s store true window_cols true retention 23h)"
	size := 5 * time.Minute
	hop := 1 * time.Minute
	lateness := 30 * time.Second
	store := true
	includeWindowCols := true
	retention := 23 * time.Hour
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&AggregateDesc{
				AggregateExprStrings: []string{"sum(f1)", "count(f2)"},
				AggregateExprs: []ExprDesc{
					&FunctionExprDesc{
						FunctionName: "sum",
						Aggregate:    true,
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{
								IdentifierName: "f1",
							},
						},
					},
					&FunctionExprDesc{
						FunctionName: "count",
						Aggregate:    true,
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{
								IdentifierName: "f2",
							},
						},
					},
				},
				KeyExprsStrings: []string{"f3", "to_lower(f4)"},
				KeyExprs: []ExprDesc{
					&IdentifierExprDesc{
						IdentifierName: "f3",
					},
					&FunctionExprDesc{
						FunctionName: "to_lower",
						Aggregate:    false,
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{
								IdentifierName: "f4",
							},
						},
					},
				},
				Size:              &size,
				Hop:               &hop,
				Lateness:          &lateness,
				Store:             &store,
				IncludeWindowCols: &includeWindowCols,
				Retention:         &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseAggregateNoBy(t *testing.T) {
	input := "my_stream := (aggregate sum(f1), count(f2) size 5m hop 1m lateness 30s store true window_cols true retention 23h)"
	size := 5 * time.Minute
	hop := 1 * time.Minute
	lateness := 30 * time.Second
	store := true
	includeWindowCols := true
	retention := 23 * time.Hour
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&AggregateDesc{
				AggregateExprStrings: []string{"sum(f1)", "count(f2)"},
				AggregateExprs: []ExprDesc{
					&FunctionExprDesc{
						FunctionName: "sum",
						Aggregate:    true,
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{
								IdentifierName: "f1",
							},
						},
					},
					&FunctionExprDesc{
						FunctionName: "count",
						Aggregate:    true,
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{
								IdentifierName: "f2",
							},
						},
					},
				},
				Size:              &size,
				Hop:               &hop,
				Lateness:          &lateness,
				Store:             &store,
				IncludeWindowCols: &includeWindowCols,
				Retention:         &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseAggregateWithEquals(t *testing.T) {
	input := "my_stream := (aggregate sum(f1), count(f2) by f3, to_lower(f4) size=5m hop=1m lateness=30s store=true window_cols=true)"
	size := 5 * time.Minute
	hop := 1 * time.Minute
	lateness := 30 * time.Second
	store := true
	includeWindowCols := true
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&AggregateDesc{
				AggregateExprStrings: []string{"sum(f1)", "count(f2)"},
				AggregateExprs: []ExprDesc{
					&FunctionExprDesc{
						FunctionName: "sum",
						Aggregate:    true,
						ArgExprs: []ExprDesc{&IdentifierExprDesc{
							IdentifierName: "f1",
						}},
					},
					&FunctionExprDesc{
						FunctionName: "count",
						Aggregate:    true,
						ArgExprs: []ExprDesc{&IdentifierExprDesc{
							IdentifierName: "f2",
						}},
					},
				},
				KeyExprsStrings: []string{"f3", "to_lower(f4)"},
				KeyExprs: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f3"},
					&FunctionExprDesc{
						FunctionName: "to_lower",
						Aggregate:    false,
						ArgExprs: []ExprDesc{
							&IdentifierExprDesc{IdentifierName: "f4"},
						},
					},
				},
				Size:              &size,
				Hop:               &hop,
				Lateness:          &lateness,
				Store:             &store,
				IncludeWindowCols: &includeWindowCols,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseAggregate(t *testing.T) {
	input := "my_stream := (aggregate)"
	expectedMsg := `there must be at least one expression (line 1 column 24):
my_stream := (aggregate)
                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate"
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 size=)"
	expectedMsg = `expected duration but found ')' (line 1 column 55):
my_stream := (aggregate sum(f1), count(f2) by f3 size=)
                                                      ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 size=foo)"
	expectedMsg = `expected duration but found 'foo' (line 1 column 55):
my_stream := (aggregate sum(f1), count(f2) by f3 size=foo)
                                                      ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 size=1000)"
	expectedMsg = `expected duration but found '1000' (line 1 column 55):
my_stream := (aggregate sum(f1), count(f2) by f3 size=1000)
                                                      ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 hop=)"
	expectedMsg = `expected duration but found ')' (line 1 column 54):
my_stream := (aggregate sum(f1), count(f2) by f3 hop=)
                                                     ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 hop=foo)"
	expectedMsg = `expected duration but found 'foo' (line 1 column 54):
my_stream := (aggregate sum(f1), count(f2) by f3 hop=foo)
                                                     ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 hop=1000)"
	expectedMsg = `expected duration but found '1000' (line 1 column 54):
my_stream := (aggregate sum(f1), count(f2) by f3 hop=1000)
                                                     ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 lateness=)"
	expectedMsg = `expected duration but found ')' (line 1 column 59):
my_stream := (aggregate sum(f1), count(f2) by f3 lateness=)
                                                          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 lateness=foo)"
	expectedMsg = `expected duration but found 'foo' (line 1 column 59):
my_stream := (aggregate sum(f1), count(f2) by f3 lateness=foo)
                                                          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 lateness=1000)"
	expectedMsg = `expected duration but found '1000' (line 1 column 59):
my_stream := (aggregate sum(f1), count(f2) by f3 lateness=1000)
                                                          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 store=)"
	expectedMsg = `expected bool but found ')' (line 1 column 56):
my_stream := (aggregate sum(f1), count(f2) by f3 store=)
                                                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 store=foo)"
	expectedMsg = `expected bool but found 'foo' (line 1 column 56):
my_stream := (aggregate sum(f1), count(f2) by f3 store=foo)
                                                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 store=1000)"
	expectedMsg = `expected bool but found '1000' (line 1 column 56):
my_stream := (aggregate sum(f1), count(f2) by f3 store=1000)
                                                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 store=TRUE)"
	expectedMsg = `expected bool but found 'TRUE' (line 1 column 56):
my_stream := (aggregate sum(f1), count(f2) by f3 store=TRUE)
                                                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 store=FALSE)"
	expectedMsg = `expected bool but found 'FALSE' (line 1 column 56):
my_stream := (aggregate sum(f1), count(f2) by f3 store=FALSE)
                                                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (aggregate sum(f1), count(f2) by f3 window_cols=foo)"
	expectedMsg = `expected bool but found 'foo' (line 1 column 62):
my_stream := (aggregate sum(f1), count(f2) by f3 window_cols=foo)
                                                             ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseStreamStreamInnerJoin(t *testing.T) {
	testParseStreamStreamJoin(t, "=")
}

func TestParseStreamStreamLeftOuterJoin(t *testing.T) {
	testParseStreamStreamJoin(t, "*=")
}

func TestParseStreamStreamRightOuterJoin(t *testing.T) {
	testParseStreamStreamJoin(t, "=*")
}

func testParseStreamStreamJoin(t *testing.T, joinType string) {
	input := fmt.Sprintf("my_stream := (join left_stream with right_stream by lf1 %s rf1 within 5m)", joinType)
	within := 5 * time.Minute
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:  "left_stream",
				RightStream: "right_stream",
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: joinType,
					},
				},
				Within: &within,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = fmt.Sprintf("my_stream := (join left_stream with right_stream by lf1 %s rf1, lf2 %s rf2, lf3 %s rf3 within 5m)",
		joinType, joinType, joinType)
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:  "left_stream",
				RightStream: "right_stream",
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: joinType,
					},
					{
						LeftCol:  "lf2",
						RightCol: "rf2",
						JoinType: joinType,
					},
					{
						LeftCol:  "lf3",
						RightCol: "rf3",
						JoinType: joinType,
					},
				},
				Within: &within,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = fmt.Sprintf("my_stream := (join left_stream with right_stream by lf1 %s rf1 within 5m retention 1h)",
		joinType)
	retention := 1 * time.Hour
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:  "left_stream",
				RightStream: "right_stream",
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: joinType,
					},
				},
				Within:    &within,
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = fmt.Sprintf("my_stream := (join left_stream with right_stream by lf1 %s rf1 within=5m retention=1h)",
		joinType)
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:  "left_stream",
				RightStream: "right_stream",
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: joinType,
					},
				},
				Within:    &within,
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseStreamTableJoin(t *testing.T) {
	input := "my_stream := (join table left_table with right_stream by lf1 = rf1)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:  "left_table",
				LeftIsTable: true,
				RightStream: "right_stream",
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: "=",
					},
				},
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (join table left_table with right_stream by lf1 =* rf1)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:  "left_table",
				LeftIsTable: true,
				RightStream: "right_stream",
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: "=*",
					},
				},
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (join left_stream with table right_table by lf1 = rf1)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:   "left_stream",
				RightStream:  "right_table",
				RightIsTable: true,
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: "=",
					},
				},
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (join left_stream with table right_table by lf1 *= rf1)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:   "left_stream",
				RightStream:  "right_table",
				RightIsTable: true,
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: "*=",
					},
				},
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (join table left_table with right_stream by lf1 = rf1 retention = 1h)"
	retention := 1 * time.Hour
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:  "left_table",
				LeftIsTable: true,
				RightStream: "right_stream",
				JoinElements: []JoinElement{
					{
						LeftCol:  "lf1",
						RightCol: "rf1",
						JoinType: "=",
					},
				},
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseJoin(t *testing.T) {
	input := "my_stream := (join)"
	expectedMsg := `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1)"
	expectedMsg = `expected 'with' but found ')' (line 1 column 26):
my_stream := (join input1)
                         ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1 with)"
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1 with input2)"
	expectedMsg = `expected 'by' but found ')' (line 1 column 38):
my_stream := (join input1 with input2)
                                     ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1 with input2 by)"
	expectedMsg = `there must be at least one join column expression (line 1 column 41):
my_stream := (join input1 with input2 by)
                                        ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1 with input2 by f1)"
	expectedMsg = `expected one of '=', '*=', '=*' but found ')' (line 1 column 44):
my_stream := (join input1 with input2 by f1)
                                           ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1 with input2 by f1 = )"
	expectedMsg = `expected identifier but found ')' (line 1 column 47):
my_stream := (join input1 with input2 by f1 = )
                                              ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1 with input2 by f1 = f2 f3 = f4)"
	expectedMsg = `expected ',' but found 'f3' (line 1 column 50):
my_stream := (join input1 with input2 by f1 = f2 f3 = f4)
                                                 ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1 with input2 by f1 = f2, f3 = f4 within foo)"
	expectedMsg = `expected '=' or duration but found 'foo' (line 1 column 66):
my_stream := (join input1 with input2 by f1 = f2, f3 = f4 within foo)
                                                                 ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (join input1 with input2 by f1 = f2, f3 = f4 within 5m retention foo)"
	expectedMsg = `expected '=' or duration but found 'foo' (line 1 column 79):
my_stream := (join input1 with input2 by f1 = f2, f3 = f4 within 5m retention foo)
                                                                              ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseStoreStream(t *testing.T) {
	input := "my_stream := (store stream)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	retention := 2 * time.Hour
	input = "my_stream := (store stream retention 2h)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&StoreStreamDesc{
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (store stream retention=2h)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&StoreStreamDesc{
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseStore(t *testing.T) {
	input := "my_stream := (store)"
	expectedMsg := `expected one of: 'stream', 'table' but found ')' (line 1 column 20):
my_stream := (store)
                   ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (store apple)"
	expectedMsg = `expected one of: 'stream', 'table' but found 'apple' (line 1 column 21):
my_stream := (store apple)
                    ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseStoreStream(t *testing.T) {
	input := "my_stream := (store stream"
	expectedMsg := `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (store stream badgers = 5m)"
	expectedMsg = `expected 'retention' but found 'badgers' (line 1 column 28):
my_stream := (store stream badgers = 5m)
                           ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (store stream retention = foo)"
	expectedMsg = `expected duration but found 'foo' (line 1 column 40):
my_stream := (store stream retention = foo)
                                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (store stream retention =)"
	expectedMsg = `expected duration but found ')' (line 1 column 39):
my_stream := (store stream retention =)
                                      ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseStoreTable(t *testing.T) {

	input := "my_stream := (store table)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&StoreTableDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (store table by f1)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&StoreTableDesc{
				KeyCols: []string{"f1"},
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (store table by f1, f2, f3)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&StoreTableDesc{
				KeyCols: []string{"f1", "f2", "f3"},
			},
		},
	}
	testParseCreateStream(t, input, expected)

	retention := 2 * time.Hour
	input = "my_stream := (store table by f1, f2, f3 retention 2h)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&StoreTableDesc{
				KeyCols:   []string{"f1", "f2", "f3"},
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (store table by f1, f2, f3 retention=2h)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&StoreTableDesc{
				KeyCols:   []string{"f1", "f2", "f3"},
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseStoreTable(t *testing.T) {
	input := "my_stream := (store table"
	expectedMsg := `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (store table retention =5m)"
	expectedMsg = `expected 'by' but found 'retention' (line 1 column 27):
my_stream := (store table retention =5m)
                          ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (store table by f1 retention = foo)"
	expectedMsg = `expected duration but found 'foo' (line 1 column 45):
my_stream := (store table by f1 retention = foo)
                                            ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (store table by f1 retention =)"
	expectedMsg = `expected duration but found ')' (line 1 column 44):
my_stream := (store table by f1 retention =)
                                           ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (store table by)"
	expectedMsg = `no key columns specified (line 1 column 29):
my_stream := (store table by)
                            ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseFilter(t *testing.T) {
	input := "my_stream := (filter by f1 > 10)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&FilterDesc{
				Expr: &BinaryOperatorExprDesc{
					Left:  &IdentifierExprDesc{IdentifierName: "f1"},
					Right: &IntegerConstExprDesc{Value: 10},
					Op:    ">",
				},
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseFilter(t *testing.T) {
	input := "my_stream := (filter)"
	expectedMsg := `expected 'by' but found ')' (line 1 column 21):
my_stream := (filter)
                    ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (filter"
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (filter by)"
	expectedMsg = `a single filter expression must be specified (line 1 column 24):
my_stream := (filter by)
                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (filter by func(f1"
	expectedMsg = `'func' is not a known function (line 1 column 25):
my_stream := (filter by func(f1
                        ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (filter by to_lower(f1"
	expectedMsg = `unmatched '(' (line 1 column 33):
my_stream := (filter by to_lower(f1
                                ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseKafaIn(t *testing.T) {
	input := "my_stream := (kafka in partitions 10)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&KafkaInDesc{
				Partitions: 10,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (kafka in partitions=10)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&KafkaInDesc{
				Partitions: 10,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream :=
(kafka in
	partitions=10
	watermark_type = processing_time
	watermark_lateness = 10s
	watermark_idle_timeout = 30s
)`
	watermarkType := "processing_time"
	watermarkLateness := 10 * time.Second
	watermarkIdleTimeout := 30 * time.Second
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&KafkaInDesc{
				Partitions:           10,
				WatermarkType:        &watermarkType,
				WatermarkLateness:    &watermarkLateness,
				WatermarkIdleTimeout: &watermarkIdleTimeout,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseKafkaIn(t *testing.T) {
	input := "my_stream := (kafka in)"
	expectedMsg := `expected 'partitions' but found ')' (line 1 column 23):
my_stream := (kafka in)
                      ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in"
	expectedMsg = `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions)"
	expectedMsg = `expected '=' or integer but found ')' (line 1 column 34):
my_stream := (kafka in partitions)
                                 ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = )"
	expectedMsg = `expected integer but found ')' (line 1 column 37):
my_stream := (kafka in partitions = )
                                    ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = foo)"
	expectedMsg = `expected integer but found 'foo' (line 1 column 37):
my_stream := (kafka in partitions = foo)
                                    ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = 10 watermark_type = foo)"
	expectedMsg = `expected one of: 'event_time', 'processing_time' but found 'foo' (line 1 column 57):
my_stream := (kafka in partitions = 10 watermark_type = foo)
                                                        ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = 10 watermark_type = processing_time watermark_lateness = foo)"
	expectedMsg = `expected duration but found 'foo' (line 1 column 94):
my_stream := (kafka in partitions = 10 watermark_type = processing_time watermark_lateness = foo)
                                                                                             ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = 10 watermark_type = processing_time watermark_idle_timeout = foo)"
	expectedMsg = `expected duration but found 'foo' (line 1 column 98):
my_stream := (kafka in partitions = 10 watermark_type = processing_time watermark_idle_timeout = foo)
                                                                                                 ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = 10 watermark_type = processing_time badgers = 10)"
	expectedMsg = `unknown argument 'badgers' (line 1 column 73):
my_stream := (kafka in partitions = 10 watermark_type = processing_time badgers = 10)
                                                                        ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestFailedToParseKafkaInDuplicateArgs(t *testing.T) {
	input := "my_stream := (kafka in partitions = 10 watermark_type = processing_time watermark_type = processing_time)"
	expectedMsg := `argument 'watermark_type' is duplicated (line 1 column 73):
my_stream := (kafka in partitions = 10 watermark_type = processing_time watermark_type = processing_time)
                                                                        ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = 10 watermark_lateness = 10m watermark_lateness = 5m)"
	expectedMsg = `argument 'watermark_lateness' is duplicated (line 1 column 65):
my_stream := (kafka in partitions = 10 watermark_lateness = 10m watermark_lateness = 5m)
                                                                ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = 10 watermark_idle_timeout = 10m watermark_idle_timeout = 5m)"
	expectedMsg = `argument 'watermark_idle_timeout' is duplicated (line 1 column 69):
my_stream := (kafka in partitions = 10 watermark_idle_timeout = 10m watermark_idle_timeout = 5m)
                                                                    ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka in partitions = 10 partitions = 10)"
	expectedMsg = `argument 'partitions' is duplicated (line 1 column 40):
my_stream := (kafka in partitions = 10 partitions = 10)
                                       ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseKafkaOut(t *testing.T) {
	input := "my_stream := (kafka out)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&KafkaOutDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (kafka out retention 5m)"
	retention := 5 * time.Minute
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&KafkaOutDesc{
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (kafka out retention=5m)"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&KafkaOutDesc{
				Retention: &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseKafkaOut(t *testing.T) {
	input := "my_stream := (kafka out"
	expectedMsg := `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka out retention)"
	expectedMsg = `expected '=' or duration but found ')' (line 1 column 34):
my_stream := (kafka out retention)
                                 ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka out retention=)"
	expectedMsg = `expected duration but found ')' (line 1 column 35):
my_stream := (kafka out retention=)
                                  ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka out retention=badgers)"
	expectedMsg = `expected duration but found 'badgers' (line 1 column 35):
my_stream := (kafka out retention=badgers)
                                  ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (kafka out badgers)"
	expectedMsg = `expected 'retention' but found 'badgers' (line 1 column 25):
my_stream := (kafka out badgers)
                        ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseTopic(t *testing.T) {

	input := "my_stream := (topic partitions 23)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&TopicDesc{
				Partitions: 23,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = "my_stream := (topic partitions 23 retention 5m)"
	retention := 5 * time.Minute
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&TopicDesc{
				Partitions: 23,
				Retention:  &retention,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (topic
	partitions 23
	retention 5m
	watermark_type processing_time
	watermark_lateness 5s
	watermark_idle_timeout 30s
)`

	watermarkLateness := 5 * time.Second
	watermarkIdleTimeout := 30 * time.Second
	watermarkType := "processing_time"
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&TopicDesc{
				Partitions:           23,
				Retention:            &retention,
				WatermarkType:        &watermarkType,
				WatermarkLateness:    &watermarkLateness,
				WatermarkIdleTimeout: &watermarkIdleTimeout,
			},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (topic
	partitions=23
	retention=5m
	watermark_type=processing_time
	watermark_lateness=5s
	watermark_idle_timeout=30s
)`

	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&TopicDesc{
				Partitions:           23,
				Retention:            &retention,
				WatermarkType:        &watermarkType,
				WatermarkLateness:    &watermarkLateness,
				WatermarkIdleTimeout: &watermarkIdleTimeout,
			},
		},
	}
	testParseCreateStream(t, input, expected)

}

func TestFailedToParseTopic(t *testing.T) {
	input := "my_stream := (topic"
	expectedMsg := `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (topic)"
	expectedMsg = `expected 'partitions' but found ')' (line 1 column 20):
my_stream := (topic)
                   ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (topic partitions=10 retention)"
	expectedMsg = `expected '=' or duration but found ')' (line 1 column 44):
my_stream := (topic partitions=10 retention)
                                           ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (topic partitions=10 retention=)"
	expectedMsg = `expected duration but found ')' (line 1 column 45):
my_stream := (topic partitions=10 retention=)
                                            ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (topic partitions=10 retention=badgers)"
	expectedMsg = `expected duration but found 'badgers' (line 1 column 45):
my_stream := (topic partitions=10 retention=badgers)
                                            ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (topic badgers)"
	expectedMsg = `expected 'partitions' but found 'badgers' (line 1 column 21):
my_stream := (topic badgers)
                    ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseUnion(t *testing.T) {
	input := "my_stream := (union stream1, stream2, stream3, stream4)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&UnionDesc{
				StreamNames: []string{"stream1", "stream2", "stream3", "stream4"},
			},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseUnion(t *testing.T) {
	input := "my_stream := (union stream1, stream2"
	expectedMsg := "reached end of statement"
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (union stream1)"
	expectedMsg = `at least two input stream names are required (line 1 column 28):
my_stream := (union stream1)
                           ^`
	testFailedToParseCreateStream(t, input, expectedMsg)

	input = "my_stream := (union)"
	expectedMsg = `at least two input stream names are required (line 1 column 20):
my_stream := (union)
                   ^`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func TestParseOperatorsWithFollowingOperator(t *testing.T) {
	input := `my_stream := (bridge from my_topic partitions = 23) -> (store stream)`
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeFromDesc{
				TopicName:  "my_topic",
				Partitions: 23,
			},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (bridge to my_topic) -> (store stream)`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BridgeToDesc{
				TopicName: "my_topic",
			},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (partition by f1 partitions = 23) -> (store stream)`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&PartitionDesc{
				KeyExprs:   []string{"f1"},
				Partitions: 23,
			},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (filter by f1 > 10) -> (store stream)`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&FilterDesc{
				Expr: &BinaryOperatorExprDesc{
					Left:  &IdentifierExprDesc{IdentifierName: "f1"},
					Right: &IntegerConstExprDesc{Value: 10},
					Op:    ">",
				},
			},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (project f3, f4, f5) -> (store stream)`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&ProjectDesc{
				Expressions: []ExprDesc{
					&IdentifierExprDesc{IdentifierName: "f3"},
					&IdentifierExprDesc{IdentifierName: "f4"},
					&IdentifierExprDesc{IdentifierName: "f5"},
				},
			},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (union s1, s2, s3) -> (store stream)`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&UnionDesc{
				StreamNames: []string{"s1", "s2", "s3"},
			},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (kafka in partitions=23) -> (store stream)`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&KafkaInDesc{
				Partitions: 23,
			},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	within := 5 * time.Minute
	input = `my_stream := (join s1 with s2 by f1=f1 within 5m) -> (store stream)`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&JoinDesc{
				LeftStream:  "s1",
				RightStream: "s2",
				JoinElements: []JoinElement{
					{
						LeftCol:  "f1",
						RightCol: "f1",
						JoinType: "=",
					},
				},
				Within: &within,
			},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)

	input = `my_stream := (backfill) -> (store stream)`
	expected = CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BackfillDesc{},
			&StoreStreamDesc{},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestParseBackfill(t *testing.T) {
	input := "my_stream := (backfill)"
	expected := CreateStreamDesc{
		StreamName: "my_stream",
		OperatorDescs: []Parseable{
			&BackfillDesc{},
		},
	}
	testParseCreateStream(t, input, expected)
}

func TestFailedToParseBackfill(t *testing.T) {
	input := "my_stream := (backfill"
	expectedMsg := `reached end of statement`
	testFailedToParseCreateStream(t, input, expectedMsg)
}

func testParseQuery(t *testing.T, input string, expected QueryDesc) {
	cs := NewQueryDesc()
	err := NewParser(nil).Parse(input, cs)
	require.NoError(t, err)

	// need to blank the BaseDesc fields
	cs.clearTokenState()

	require.Equal(t, &expected, cs)
}

func testFailedToParseQuery(t *testing.T, input string, expectedMsg string) {
	cs := NewQueryDesc()
	err := NewParser(nil).Parse(input, cs)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.ParseError))
	require.Equal(t, expectedMsg, err.Error())
}

func TestParseGet(t *testing.T) {

	input := `(get "key123" from some_table)`
	expected := QueryDesc{OperatorDescs: []Parseable{
		&GetDesc{
			KeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: `key123`},
			},
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(get "cust1234", 100, true from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&GetDesc{
			KeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: `cust1234`},
				&IntegerConstExprDesc{Value: 100},
				&BoolConstExprDesc{Value: true},
			},
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(get $p1, $p2, $p3 from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&GetDesc{
			KeyExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: `$p1`},
				&IdentifierExprDesc{IdentifierName: `$p2`},
				&IdentifierExprDesc{IdentifierName: `$p3`},
			},
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)
}

func TestFailedToParseGet(t *testing.T) {
	input := `(get`
	expectedMsg := `reached end of statement`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(get)`
	expectedMsg = `there must be at least one key expression (line 1 column 5):
(get)
    ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(get "val1")`
	expectedMsg = `expected 'from' but found ')' (line 1 column 12):
(get "val1")
           ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(get "val1" from)`
	expectedMsg = `expected identifier but found ')' (line 1 column 17):
(get "val1" from)
                ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(get "val1" from 100)`
	expectedMsg = `expected identifier but found '100' (line 1 column 18):
(get "val1" from 100)
                 ^`
	testFailedToParseQuery(t, input, expectedMsg)
}

func TestParseScanAll(t *testing.T) {
	input := `(scan all from some_table)`
	expected := QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			All:       true,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)
}

func TestParseScan(t *testing.T) {
	input := `(scan "val1" to "val9" from some_table)`
	expected := QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			FromKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "val1"},
			},
			ToKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "val9"},
			},
			FromIncl:  true,
			ToIncl:    false,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan "start1", "start2", 100 to "end1", "end2", 200 from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			FromKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "start1"},
				&StringConstExprDesc{Value: "start2"},
				&IntegerConstExprDesc{Value: 100},
			},
			ToKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "end1"},
				&StringConstExprDesc{Value: "end2"},
				&IntegerConstExprDesc{Value: 200},
			},
			FromIncl:  true,
			ToIncl:    false,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan "start1", "start2", 100 incl to "end1", "end2", 200 excl from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			FromKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "start1"},
				&StringConstExprDesc{Value: "start2"},
				&IntegerConstExprDesc{Value: 100},
			},
			ToKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "end1"},
				&StringConstExprDesc{Value: "end2"},
				&IntegerConstExprDesc{Value: 200},
			},
			FromIncl:  true,
			ToIncl:    false,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan "start1", "start2", 100 excl to "end1", "end2", 200 incl from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			FromKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "start1"},
				&StringConstExprDesc{Value: "start2"},
				&IntegerConstExprDesc{Value: 100},
			},
			ToKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "end1"},
				&StringConstExprDesc{Value: "end2"},
				&IntegerConstExprDesc{Value: 200},
			},
			FromIncl:  false,
			ToIncl:    true,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan start to "end1", "end2", 200 from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			ToKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "end1"},
				&StringConstExprDesc{Value: "end2"},
				&IntegerConstExprDesc{Value: 200},
			},
			FromIncl:  true,
			ToIncl:    false,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan start to "end1", "end2", 200 incl from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			ToKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "end1"},
				&StringConstExprDesc{Value: "end2"},
				&IntegerConstExprDesc{Value: 200},
			},
			FromIncl:  true,
			ToIncl:    true,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan start excl to "end1", "end2", 200 from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			ToKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "end1"},
				&StringConstExprDesc{Value: "end2"},
				&IntegerConstExprDesc{Value: 200},
			},
			FromIncl:  false,
			ToIncl:    false,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan "start1", "start2", 100 to end from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			FromKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "start1"},
				&StringConstExprDesc{Value: "start2"},
				&IntegerConstExprDesc{Value: 100},
			},
			FromIncl:  true,
			ToIncl:    false,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan "start1", "start2", 100 excl to end from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			FromKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "start1"},
				&StringConstExprDesc{Value: "start2"},
				&IntegerConstExprDesc{Value: 100},
			},
			FromIncl:  false,
			ToIncl:    false,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)

	input = `(scan "start1", "start2", 100 to end incl from some_table)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			FromKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "start1"},
				&StringConstExprDesc{Value: "start2"},
				&IntegerConstExprDesc{Value: 100},
			},
			FromIncl:  true,
			ToIncl:    true,
			All:       false,
			TableName: "some_table",
		},
	}}
	testParseQuery(t, input, expected)
}

func TestFailedToParseScan(t *testing.T) {
	input := `(scan`
	expectedMsg := `reached end of statement`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan)`
	expectedMsg = `reached end of statement`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan all)`
	expectedMsg = `expected 'from' but found ')' (line 1 column 10):
(scan all)
         ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan all from)`
	expectedMsg = `expected identifier but found ')' (line 1 column 15):
(scan all from)
              ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan all from 100)`
	expectedMsg = `expected identifier but found '100' (line 1 column 16):
(scan all from 100)
               ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan "val1" from some_table)`
	expectedMsg = `expected one of: 'to', 'excl', 'incl' but found 'from' (line 1 column 14):
(scan "val1" from some_table)
             ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan "val1")`
	expectedMsg = `expected one of: 'to', 'excl', 'incl' but found ')' (line 1 column 13):
(scan "val1")
            ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan "val1" from)`
	expectedMsg = `expected one of: 'to', 'excl', 'incl' but found 'from' (line 1 column 14):
(scan "val1" from)
             ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan "val1" to)`
	expectedMsg = `there must be at least one expression (line 1 column 16):
(scan "val1" to)
               ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan "val1" to "val2")`
	expectedMsg = `expected one of: 'from', 'excl', 'incl' but found ')' (line 1 column 23):
(scan "val1" to "val2")
                      ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan "val1" to "val2" from)`
	expectedMsg = `expected identifier but found ')' (line 1 column 28):
(scan "val1" to "val2" from)
                           ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(scan "val1" to "val2" from 100)`
	expectedMsg = `expected identifier but found '100' (line 1 column 29):
(scan "val1" to "val2" from 100)
                            ^`
	testFailedToParseQuery(t, input, expectedMsg)
}

func TestParseSort(t *testing.T) {
	input := `(sort by f1)`
	expected := QueryDesc{OperatorDescs: []Parseable{
		&SortDesc{
			SortExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
			},
		},
	}}
	testParseQuery(t, input, expected)

	input = `(sort by f1, f2, f3)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&SortDesc{
			SortExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f1"},
				&IdentifierExprDesc{IdentifierName: "f2"},
				&IdentifierExprDesc{IdentifierName: "f3"},
			},
		},
	}}
	testParseQuery(t, input, expected)

	input = `(sort by to_lower(f1), f2 + 10, to_string(f3))`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&SortDesc{
			SortExprs: []ExprDesc{
				&FunctionExprDesc{
					FunctionName: "to_lower",
					Aggregate:    false,
					ArgExprs:     []ExprDesc{&IdentifierExprDesc{IdentifierName: "f1"}},
				},
				&BinaryOperatorExprDesc{
					Left:  &IdentifierExprDesc{IdentifierName: "f2"},
					Right: &IntegerConstExprDesc{Value: 10},
					Op:    "+",
				},
				&FunctionExprDesc{
					FunctionName: "to_string",
					Aggregate:    false,
					ArgExprs:     []ExprDesc{&IdentifierExprDesc{IdentifierName: "f3"}},
				},
			},
		},
	}}
	testParseQuery(t, input, expected)
}

func TestFailedToParseSort(t *testing.T) {
	input := `(sort`
	expectedMsg := `reached end of statement`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(sort)`
	expectedMsg = `expected 'by' but found ')' (line 1 column 6):
(sort)
     ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(sort by)`
	expectedMsg = `at least one sort expression must be specified (line 1 column 9):
(sort by)
        ^`
	testFailedToParseQuery(t, input, expectedMsg)

	input = `(sort fly)`
	expectedMsg = `expected 'by' but found 'fly' (line 1 column 7):
(sort fly)
      ^`
	testFailedToParseQuery(t, input, expectedMsg)
}

func TestMultipleQueryOperators(t *testing.T) {
	input := `(scan "val1" to "val2" from some_table)->(filter by f1 > 10)->(project f3, f7)->(sort by f7, f3)`
	expected := QueryDesc{OperatorDescs: []Parseable{
		&ScanDesc{
			FromKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "val1"},
			},
			ToKeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "val2"},
			},
			ToIncl:    false,
			FromIncl:  true,
			TableName: "some_table",
			All:       false,
		},
		&FilterDesc{
			Expr: &BinaryOperatorExprDesc{
				Left:  &IdentifierExprDesc{IdentifierName: "f1"},
				Right: &IntegerConstExprDesc{Value: 10},
				Op:    ">",
			},
		},
		&ProjectDesc{
			Expressions: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f3"},
				&IdentifierExprDesc{IdentifierName: "f7"},
			},
		},
		&SortDesc{
			SortExprs: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f7"},
				&IdentifierExprDesc{IdentifierName: "f3"},
			},
		},
	}}
	testParseQuery(t, input, expected)

	input = `(get "val1" from some_table)->(filter by f1 > 10)->(project f3, f7)`
	expected = QueryDesc{OperatorDescs: []Parseable{
		&GetDesc{
			KeyExprs: []ExprDesc{
				&StringConstExprDesc{Value: "val1"},
			},
			TableName: "some_table",
		},
		&FilterDesc{
			Expr: &BinaryOperatorExprDesc{
				Left:  &IdentifierExprDesc{IdentifierName: "f1"},
				Right: &IntegerConstExprDesc{Value: 10},
				Op:    ">",
			},
		},
		&ProjectDesc{
			Expressions: []ExprDesc{
				&IdentifierExprDesc{IdentifierName: "f3"},
				&IdentifierExprDesc{IdentifierName: "f7"},
			},
		},
	}}
	testParseQuery(t, input, expected)
}

func testParseDeleteStream(t *testing.T, input string, expected DeleteStreamDesc) {
	cs := NewDeleteStreamDesc()
	err := NewParser(nil).Parse(input, cs)
	require.NoError(t, err)
	cs.clearTokenState()
	require.Equal(t, &expected, cs)
}

func testFailedToParseDeleteStream(t *testing.T, input string, expectedMsg string) {
	cs := NewDeleteStreamDesc()
	err := NewParser(nil).Parse(input, cs)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.ParseError))
	require.Equal(t, expectedMsg, err.Error())
}

func TestParseDeleteStream(t *testing.T) {
	input := "delete(my_stream)"
	expected := DeleteStreamDesc{
		StreamName: "my_stream",
	}
	testParseDeleteStream(t, input, expected)
}

func TestFailedToParseDeleteStream(t *testing.T) {
	input := "delete"
	expectedMsg := "reached end of statement"
	testFailedToParseDeleteStream(t, input, expectedMsg)

	input = "delete("
	expectedMsg = "reached end of statement"
	testFailedToParseDeleteStream(t, input, expectedMsg)

	input = "delete()"
	expectedMsg = `expected identifier but found ')' (line 1 column 8):
delete()
       ^`
	testFailedToParseDeleteStream(t, input, expectedMsg)

	input = "delete(my_stream"
	expectedMsg = `reached end of statement`
	testFailedToParseDeleteStream(t, input, expectedMsg)

	input = `delete("my_stream")`
	expectedMsg = `expected identifier but found '"my_stream"' (line 1 column 8):
delete("my_stream")
       ^`
	testFailedToParseDeleteStream(t, input, expectedMsg)
}

func testParseTSL(t *testing.T, input string, expected TSLDesc) {
	cs := NewTSLDesc()
	err := NewParser(nil).Parse(input, cs)
	require.NoError(t, err)
	cs.clearTokenState()
	require.Equal(t, &expected, cs)
}

func testFailedToParseTSL(t *testing.T, input string, expectedMsg string) {
	cs := NewTSLDesc()
	err := NewParser(nil).Parse(input, cs)
	require.Error(t, err)
	require.True(t, common.IsTektiteErrorWithCode(err, common.ParseError))
	require.Equal(t, expectedMsg, err.Error())
}

func TestParseTSL(t *testing.T) {
	input := "my_stream := (bridge from some_topic partitions=10 props=())->(store stream)"
	expected := TSLDesc{
		CreateStream: &CreateStreamDesc{
			StreamName: "my_stream",
			OperatorDescs: []Parseable{
				&BridgeFromDesc{
					TopicName:  "some_topic",
					Partitions: 10,
					Props:      map[string]string{},
				},
				&StoreStreamDesc{},
			},
		},
	}
	testParseTSL(t, input, expected)

	input = "delete(my_stream)"
	expected = TSLDesc{
		DeleteStream: &DeleteStreamDesc{
			StreamName: "my_stream",
		},
	}
	testParseTSL(t, input, expected)

	input = `list()`
	expected = TSLDesc{ListStreams: &ListStreamsDesc{}}
	testParseTSL(t, input, expected)

	input = `list("foo.*")`
	expected = TSLDesc{ListStreams: &ListStreamsDesc{RegEx: `"foo.*"`}}
	testParseTSL(t, input, expected)

	input = `show(my_stream)`
	expected = TSLDesc{ShowStream: &ShowStreamDesc{StreamName: "my_stream"}}
	testParseTSL(t, input, expected)
}

func TestParsePrepare(t *testing.T) {
	input := `prepare my_query := (get $key1:int, $key2:float, $key3:bool, $key4:decimal(23,7), $key5:string, $key6:bytes, $key7:timestamp from some_table)->(filter by $key8:bool)`
	expected := TSLDesc{
		PrepareQuery: &PrepareQueryDesc{
			QueryName: "my_query",
			Query: &QueryDesc{
				OperatorDescs: []Parseable{
					&GetDesc{
						KeyExprs: []ExprDesc{
							&IdentifierExprDesc{IdentifierName: "$key1:int"},
							&IdentifierExprDesc{IdentifierName: "$key2:float"},
							&IdentifierExprDesc{IdentifierName: "$key3:bool"},
							&IdentifierExprDesc{IdentifierName: "$key4:decimal(23,7)"},
							&IdentifierExprDesc{IdentifierName: "$key5:string"},
							&IdentifierExprDesc{IdentifierName: "$key6:bytes"},
							&IdentifierExprDesc{IdentifierName: "$key7:timestamp"},
						},
						TableName: "some_table",
					},
					&FilterDesc{
						Expr: &IdentifierExprDesc{IdentifierName: "$key8:bool"},
					},
				},
			},
			Params: []PreparedStatementParam{
				{
					ParamName: "$key1:int",
					ParamType: types.ColumnTypeInt,
				},
				{
					ParamName: "$key2:float",
					ParamType: types.ColumnTypeFloat,
				},
				{
					ParamName: "$key3:bool",
					ParamType: types.ColumnTypeBool,
				},
				{
					ParamName: "$key4:decimal(23,7)",
					ParamType: &types.DecimalType{
						Precision: 23,
						Scale:     7,
					},
				},
				{
					ParamName: "$key5:string",
					ParamType: types.ColumnTypeString,
				},
				{
					ParamName: "$key6:bytes",
					ParamType: types.ColumnTypeBytes,
				},
				{
					ParamName: "$key7:timestamp",
					ParamType: types.ColumnTypeTimestamp,
				},
				{
					ParamName: "$key8:bool",
					ParamType: types.ColumnTypeBool,
				},
			},
		},
	}
	testParseTSL(t, input, expected)
}

func TestFailedToParseTSL(t *testing.T) {
	expectedMsg := `statement is empty`
	testFailedToParseTSL(t, ``, expectedMsg)

	input := `foo`
	expectedMsg = `reached end of statement`
	testFailedToParseTSL(t, input, expectedMsg)
}
