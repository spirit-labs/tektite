package cli

import (
	"fmt"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/tekclient"
	"github.com/spirit-labs/tektite/types"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spirit-labs/tektite/errors"
)

const (
	maxBufferedLines     = 1000
	defaultMaxLineWidth  = 120
	minLineWidth         = 10
	minColWidth          = 5
	maxLineWidthPropName = "max_line_width"
	maxLineWidth         = 10000
	queryBatchSize       = 10000
)

type Cli struct {
	lock          sync.Mutex
	started       bool
	serverAddress string
	client        tekclient.Client
	batchSize     int
	maxLineWidth  int
	tlsConfig     tekclient.TLSConfig
	exitOnError   bool
}

func NewCli(serverAddress string, tlsConfig tekclient.TLSConfig) *Cli {
	return &Cli{
		serverAddress: serverAddress,
		batchSize:     queryBatchSize,
		maxLineWidth:  defaultMaxLineWidth,
		tlsConfig:     tlsConfig,
	}
}

func (c *Cli) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	var err error
	c.client, err = tekclient.NewClient(c.serverAddress, c.tlsConfig)
	if err != nil {
		return err
	}
	c.started = true
	return nil
}

func (c *Cli) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	c.client.Close()
	c.started = false
	return nil
}

func (c *Cli) SetPageSize(pageSize int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.batchSize = pageSize
}

func (c *Cli) SetExitOnError(exitOnError bool) {
	c.exitOnError = exitOnError
}

func (c *Cli) ExecuteStatement(statement string) (chan string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil, errors.Error("not started")
	}
	statement = strings.TrimSuffix(statement, ";")
	statement = strings.TrimLeft(statement, " \t")
	ch := make(chan string, maxBufferedLines)
	go c.doExecuteStatement(statement, ch)
	return ch, nil
}

func (c *Cli) doExecuteStatement(statement string, ch chan string) {
	if rc, showResult, err := c.doExecuteStatementWithError(statement, ch); err != nil {
		ch <- c.checkErrorAndMaybeExit(err).Error()
	} else {
		if showResult {
			if rc == -1 {
				// not a query
				ch <- "OK"
			} else if rc == 1 {
				ch <- "1 row returned"
			} else {
				ch <- fmt.Sprintf("%d rows returned", rc)
			}
		}
	}
	close(ch)
}

func (c *Cli) handleSetCommand(statement string) error {
	parts := strings.Split(statement, " ")
	if len(parts) != 3 {
		return errors.Error("Invalid set command. Should be set <prop_name> <prop_value>")
	}
	if propName := parts[1]; propName == maxLineWidthPropName {
		propVal := parts[2]
		width, err := strconv.Atoi(propVal)
		if err != nil || width < minLineWidth || width > maxLineWidth {
			return errors.Errorf("Invalid %s value: %s", maxLineWidthPropName, propVal)
		}
		c.lock.Lock()
		defer c.lock.Unlock()
		c.maxLineWidth = width
	} else {
		return errors.Errorf("Unknown property: %s", propName)
	}
	return nil
}

func (c *Cli) handleRegisterWasm(statement string) error {
	if !strings.HasPrefix(statement, `register_wasm("`) || !strings.HasSuffix(statement, `")`) {
		return errors.Errorf(`Invalid register_wasm command. Must be of form 'register_wasm("/path/to/my_module.wasm")'`)
	}
	modulePath := statement[15 : len(statement)-2]
	return c.client.RegisterWasmModule(modulePath)
}

func (c *Cli) handleUnregisterWasm(statement string) error {
	if !strings.HasPrefix(statement, `unregister_wasm("`) || !strings.HasSuffix(statement, `")`) {
		return errors.Errorf(`Invalid unregister_wasm command. Must be of form 'unregister_wasm("my_module")'`)
	}
	moduleName := statement[17 : len(statement)-2]
	return c.client.UnregisterWasmModule(moduleName)
}

func (c *Cli) doExecuteStatementWithError(statement string, out chan string) (int, bool, error) {
	lowerStat := strings.ToLower(statement)
	if lowerStat == "set" || strings.HasPrefix(lowerStat, "set ") {
		return -1, true, c.handleSetCommand(lowerStat)
	}
	if strings.HasPrefix(lowerStat, "register_wasm(") {
		return -1, true, c.handleRegisterWasm(lowerStat)
	}
	if strings.HasPrefix(lowerStat, "unregister_wasm(") {
		return -1, true, c.handleUnregisterWasm(lowerStat)
	}
	if strings.HasPrefix(statement, "(") {
		ch, err := c.client.StreamExecuteQuery(statement)
		if err != nil {
			return 0, true, err
		}
		return c.streamToOut(out, ch, true), true, nil
	}
	p := parser.NewParser(nil)
	tsl, _ := p.ParseTSL(statement)
	if tsl.ListStreams != nil {
		// list is just an alias for a query against sys.streams
		if tsl.ListStreams.RegEx != "" {
			statement = fmt.Sprintf(`(scan all from sys.streams)->(filter by matches(stream_name,%s))->(project stream_name)->(sort by stream_name)`, tsl.ListStreams.RegEx)
		} else {
			statement = `(scan all from sys.streams)->(project stream_name)->(sort by stream_name)`
		}
		ch, err := c.client.StreamExecuteQuery(statement)
		if err != nil {
			return 0, true, err
		}
		return c.streamToOut(out, ch, true), true, nil
	} else if tsl.ShowStream != nil {
		// show stream executes a get on the sys.stream table and formats the results in an easy-to-read way on
		// multiple lines
		query := fmt.Sprintf(`(get "%s" from sys.streams)`, tsl.ShowStream.StreamName)
		qr, err := c.client.ExecuteQuery(query)
		if err != nil {
			return 0, true, err
		}
		if qr.RowCount() == 0 {
			out <- "unknown stream"
			return 0, false, nil
		}
		row := qr.Row(0)
		out <- ""
		out <- "stream_name:   " + row.StringVal(0)
		out <- "stream_def:    " + row.StringVal(1)
		out <- fmt.Sprintf("in_schema:     {%s} partitions: %d mapping_id: %s", row.StringVal(2), row.IntVal(3), row.StringVal(4))
		out <- fmt.Sprintf("out_schema:    {%s} partitions: %d mapping_id: %s", row.StringVal(5), row.IntVal(6), row.StringVal(7))
		hasChildren := !row.IsNull(8)
		childStreams := "[none]"
		if hasChildren {
			childStreams = row.StringVal(8)
		}
		out <- fmt.Sprintf("child_streams: %s", childStreams)
		out <- ""
		return 0, false, nil
	}
	err := c.client.ExecuteStatement(statement)
	return -1, true, err
}

func (c *Cli) streamToOut(out chan string, ch chan tekclient.StreamChunk, isQuery bool) int {
	rowCount := 0
	first := true
	var columnWidths []int
	var headerBorder string
	for chunk := range ch {
		if chunk.Err != nil {
			out <- c.checkErrorAndMaybeExit(chunk.Err).Error()
			return rowCount
		}
		if first {
			// First we write out the column header
			columnTypes := chunk.Chunk.Meta().ColumnTypes()
			columnNames := chunk.Chunk.Meta().ColumnNames()
			columnWidths = c.calcColumnWidths(columnTypes, columnNames)
			header := c.writeHeader(columnNames, columnWidths)
			headerBorder = createHeaderBorder(len(header))
			out <- headerBorder
			out <- header
			out <- headerBorder
			first = false
		}
		for rowIndex := 0; rowIndex < chunk.Chunk.RowCount(); rowIndex++ {
			line, err := formatLine(chunk.Chunk, rowIndex, columnWidths)
			if err != nil {
				out <- c.checkErrorAndMaybeExit(chunk.Err).Error()
				return 0
			}
			out <- line
			rowCount++
		}
	}
	if rowCount > 0 {
		out <- headerBorder
	}
	if isQuery {
		return rowCount
	} else {
		return -1
	}
}

func (c *Cli) writeHeader(columnNames []string, columnWidths []int) string {
	sb := &strings.Builder{}
	sb.WriteString("|")
	for i, v := range columnNames {
		sb.WriteRune(' ')
		cw := columnWidths[i]
		if len(v) > cw {
			v = v[:cw-2] + ".."
		}
		sb.WriteString(rightPadToWidth(cw, v))
		sb.WriteString(" |")
	}
	return sb.String()
}

func createHeaderBorder(headerLen int) string {
	return "+" + strings.Repeat("-", headerLen-2) + "+"
}

func rightPadToWidth(width int, s string) string {
	padSpaces := width - len(s)
	pad := strings.Repeat(" ", padSpaces)
	s += pad
	return s
}

func convertUnixMillisToDateString(unixTime int64) string {
	gt := time.UnixMilli(unixTime).In(time.UTC)
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%06d",
		gt.Year(), gt.Month(), gt.Day(), gt.Hour(), gt.Minute(), gt.Second(), gt.Nanosecond()/1000)
}

func formatLine(res tekclient.QueryResult, rowIndex int, colWidths []int) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString("|")
	row := res.Row(rowIndex)
	for i := 0; i < len(colWidths); i++ {
		sb.WriteRune(' ')
		var v string
		if row.IsNull(i) {
			v = "null"
		} else {
			switch res.Meta().ColumnTypes()[i].ID() {
			case types.ColumnTypeIDInt:
				v = strconv.Itoa(int(row.IntVal(i)))
			case types.ColumnTypeIDFloat:
				v = strconv.FormatFloat(row.FloatVal(i), 'f', 6, 64)
			case types.ColumnTypeIDBool:
				b := row.BoolVal(i)
				if b {
					v = "true"
				} else {
					v = "false"
				}
			case types.ColumnTypeIDDecimal:
				d := row.DecimalVal(i)
				v = d.String()
			case types.ColumnTypeIDString:
				v = row.StringVal(i)
			case types.ColumnTypeIDBytes:
				v = convBytesToString(row.BytesVal(i))
			case types.ColumnTypeIDTimestamp:
				ts := row.TimestampVal(i)
				v = convertUnixMillisToDateString(ts.Val)
			default:
				panic("unexpected type")
			}
		}
		cw := colWidths[i]
		if len(v) > cw {
			v = v[:cw-2] + ".."
		}
		sb.WriteString(rightPadToWidth(cw, v))
		sb.WriteString(" |")
	}
	return sb.String(), nil
}

func convBytesToString(bytes []byte) string {
	lb := len(bytes)
	out := make([]byte, lb)
	for i, b := range bytes {
		if b >= 32 && b <= 126 {
			// we only display printable ASCII chars
			out[i] = b
		} else {
			out[i] = '.'
		}
	}
	return common.ByteSliceToStringZeroCopy(out)
}

func (c *Cli) calcColumnWidths(colTypes []types.ColumnType, colNames []string) []int {
	l := len(colTypes)
	if l == 0 {
		return []int{}
	}
	colWidths := make([]int, l)
	var freeCols []int
	availWidth := c.maxLineWidth - 1
	// We try to give the full col width to any cols with a fixed max size
	for i, colType := range colTypes {
		w := 0
		switch colType.ID() {
		case types.ColumnTypeIDInt:
			w = 20
		case types.ColumnTypeIDTimestamp:
			w = 26
		default:
			// We consider these free columns
			freeCols = append(freeCols, i)
		}
		if w != 0 {
			if len(colNames[i]) > w {
				w = len(colNames[i])
			}
			colWidths[i] = w
			availWidth -= w + 3
			if availWidth < 0 {
				break
			}
		}
	}
	if availWidth < 0 {
		// Fall back to just splitting up all columns evenly
		return c.calcEvenColWidths(l)
	} else if len(freeCols) > 0 {
		// For each free column we give it an equal share of what is remaining
		freeColWidth := (availWidth / len(freeCols)) - 3
		if freeColWidth < minColWidth {
			// Fall back to just splitting up all columns evenly
			return c.calcEvenColWidths(l)
		}
		for _, freeCol := range freeCols {
			colWidths[freeCol] = freeColWidth
		}
	}

	return colWidths
}

func (c *Cli) calcEvenColWidths(numCols int) []int {
	colWidth := (c.maxLineWidth - 3*numCols - 1) / numCols
	if colWidth < minColWidth {
		colWidth = minColWidth
	}
	colWidths := make([]int, numCols)
	for i := range colWidths {
		colWidths[i] = colWidth
	}
	return colWidths
}

func (c *Cli) checkErrorAndMaybeExit(err error) error {
	var terr errors.TektiteError
	if errors.As(err, &terr) {
		// It's a Tektite error
		return err
	}
	if c.exitOnError {
		log.Errorf("connection error. Will exit. %v", err)
		os.Exit(1)
		return nil
	}
	return errors.Errorf("connection error: %v", err)
}
