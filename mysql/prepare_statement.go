package mysql

import (
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"regexp"
	"fmt"
	"github.com/sgoby/sqlparser/sqltypes"
	"math"
	"strconv"
	"github.com/golang/glog"
	"time"
)

//
type PrepareStatement struct {
	Id         uint32
	Numparams  uint16
	Numcolumns uint16
	Columns    []*querypb.Field
	Params     []*querypb.Field
	Query      string
}

//
func (c *Conn) parseComPrepare(data []byte) (string, *PrepareStatement, error) {
	query := string(data[1:])
	reg, err := regexp.Compile("\\?{1}")
	if err != nil {
		return query, nil, err
	}
	stmt := new(PrepareStatement)
	arr := reg.FindAllString(query, -1)
	if arr != nil {
		stmt.Numparams = uint16(len(arr))
		for range arr {
			stmt.Params = append(stmt.Params, &querypb.Field{Name: "?"})
		}
	}
	stmt.Query = query
	return query, stmt, nil
}

//
func (c *Conn) writePrepare(stmt *PrepareStatement) error {
	length := 1 + // OKPacket
		4 +
		2 +
		2 +
		1 + // filter [00]
		2 // warnings
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, OKPacket)
	pos = writeUint32(data, pos, stmt.Id)
	//pos = writeLenEncInt(data, pos, stmt.Id)
	pos = writeUint16(data, pos, stmt.Numcolumns)
	pos = writeUint16(data, pos, stmt.Numparams)
	pos = writeByte(data, pos, 0)
	pos = writeUint16(data, pos, 0)
	//
	if err := c.writeEphemeralPacket(false); err != nil {
		return err
	}
	//
	for _, param := range stmt.Params {
		if err := c.writeColumnDefinition(param); err != nil {
			return err
		}
	}
	// Now send each Field.
	for _, field := range stmt.Columns {
		if err := c.writeColumnDefinition(field); err != nil {
			return err
		}
	}
	//
	if len(stmt.Params) > 0 || len(stmt.Columns) > 0 {
		// Now send an EOF packet.
		if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
			// With CapabilityClientDeprecateEOF, we do not send this EOF.
			if err := c.writeEOFPacket(c.StatusFlags, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeEndResult concludes the sending of a Result.
func (c *Conn) writeEndPrepare() error {
	// Send either an EOF, or an OK packet.
	// FIXME(alainjobart) if multi result is set, can send more after this.
	// See doc.go.
	if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
		//if err := c.writeEOFPacket(c.StatusFlags, 0); err != nil {
		//	return err
		//}
		if err := c.flush(); err != nil {
			return err
		}
	} else {
		// This will flush too.
		if err := c.writeOKPacketWithEOFHeader(0, 0, c.StatusFlags, 0); err != nil {
			return err
		}
	}

	return nil
}

//bind parameters
func (this *PrepareStatement) bindParams(paramValues []sqltypes.Value) (query string, err error) {
	if len(paramValues) < 1{
		return this.Query,nil
	}
	reg, err := regexp.Compile("\\?{1}")
	if err != nil {
		return "", err
	}
	//
	index := 0
	queryNew := reg.ReplaceAllStringFunc(this.Query, func(old string) string {
		if index < len(paramValues) {
			p := paramValues[index]
			index += 1
			switch p.Type() {
			case sqltypes.Uint8, sqltypes.Int16, sqltypes.Uint16, sqltypes.Int32, sqltypes.Uint32,
				sqltypes.Int64, sqltypes.Uint64, sqltypes.Int24, sqltypes.Uint24:
				return fmt.Sprintf("%s", p.ToString())
			case sqltypes.Float32, sqltypes.Float64:
				return fmt.Sprintf("%s", p.ToString())
			default:
				return fmt.Sprintf("'%s'", p.ToString())
			}
		}
		err = fmt.Errorf("missing parameters")
		return "?"
	})
	return queryNew, err
}

//=============================
//
type PrepareExecute struct {
	StatementId uint32
	//CURSOR_TYPE_NO_CURSOR 0x00,
	// CURSOR_TYPE_READ_ONLY,
	// CURSOR_TYPE_FOR_UPDATE,
	// CURSOR_TYPE_SCROLLABLE
	Flags          byte
	IterationCount uint32
	NullBitmaps    []byte
	ParamTypes     []querypb.Type //querypb.Type
	ParamValues    []sqltypes.Value
}

func (c *Conn) parseComPrepareExecute(data []byte) (*PrepareExecute, error) {
	pos := 1
	prepareExecute := new(PrepareExecute)
	prepareExecute.StatementId, pos, _ = readUint32(data, pos)
	prepareExecute.Flags, pos, _ = readByte(data, pos)
	prepareExecute.IterationCount, pos, _ = readUint32(data, pos)
	//
	stmt, ok := c.statementMap[prepareExecute.StatementId]
	if !ok {
		return nil, fmt.Errorf("no statement")
	}
	//NULL-bitmap, length: (num-params+7)/8
	nullBitmapLen := (stmt.Numparams + 7) / 8
	//
	if nullBitmapLen < 1{
		return prepareExecute,nil
	}
	//
	if len(data) < (pos + int(nullBitmapLen) + 1) {
		return nil, fmt.Errorf("data len less %d", pos+int(nullBitmapLen)+1)
	}
	prepareExecute.NullBitmaps, pos, _ = readBytes(data, pos, int(nullBitmapLen)) // data[pos : pos + int(nullBitmapLen)]
	//
	paramsBoundFlag, pos, _ := readByte(data, pos)
	//
	if paramsBoundFlag == 1 {
		//type of each parameter, length: num-params * 2
		typeLen := stmt.Numparams * 2
		if len(data) < (pos + int(typeLen) + 1) {
			return nil, fmt.Errorf("data len less %d", pos+int(typeLen)+1)
		}

		var typeBuf []byte
		typeBuf, pos, _ = readBytes(data, pos, int(typeLen))
		//
		vBuf := data[pos:]
		//
		var ok bool
		var dpos int
		var val sqltypes.Value
		dpos = 0
		//
		for tpos := 0; uint16(tpos) < typeLen; tpos += 2 {
			t, _, tok := readUint16(typeBuf, tpos)
			if !tok {
				return nil, fmt.Errorf("can not parse value type")
			}
			pt, err := sqltypes.MySQLToType(int64(t), 1)
			if err != nil {
				return nil, fmt.Errorf("can not parse value type")
			}
			prepareExecute.ParamTypes = append(prepareExecute.ParamTypes, pt)
			val, dpos, ok = parseVale(pt, vBuf, dpos)
			if !ok {
				return nil, fmt.Errorf("can not parse value %v", vBuf)
			}
			prepareExecute.ParamValues = append(prepareExecute.ParamValues, val)
		}
	}
	return prepareExecute, nil
}

//
func parseVale(pTyp querypb.Type, buf []byte, pos int) (sqltypes.Value, int, bool) {
	mTyp, _ := sqltypes.TypeToMySQL(pTyp)
	if mTyp > 16 {
		s, dpos, ok := readLenEncStringAsBytes(buf, pos)
		v := sqltypes.MakeTrusted(pTyp, s)
		return v, dpos, ok
	}
	var v sqltypes.Value
	//littleEndian := buf[pos: pos+ int(mTyp)]
	switch pTyp {
	case sqltypes.Uint8: //{typ: 1, flags: mysqlUnsigned},
	case sqltypes.Int16, sqltypes.Uint16: //{typ: 2},{typ: 2, flags: mysqlUnsigned},
		num, dpos, _ := readUint16(buf, pos)
		pos = dpos
		v = sqltypes.NewInt32(int32(num))
	case sqltypes.Int32, sqltypes.Uint32: //{typ: 3},{typ: 3, flags: mysqlUnsigned},
		num, dpos, _ := readUint32(buf, pos)
		pos = dpos
		v = sqltypes.NewInt32(int32(num))
	case sqltypes.Float32: //{typ: 4},
		bits, dpos, _ := readUint32(buf, pos)
		pos = dpos
		fl := math.Float32frombits(bits)
		v = sqltypes.MakeTrusted(querypb.Type_FLOAT32, []byte(fmt.Sprint(fl)))
	case sqltypes.Float64: //{typ: 5},
		bits, dpos, _ := readUint64(buf, pos)
		pos = dpos
		fl := math.Float64frombits(bits)
		v = sqltypes.MakeTrusted(querypb.Type_FLOAT64, []byte(fmt.Sprint(fl)))
	case sqltypes.Null: //{typ: 6, flags: mysqlBinary},
		v = sqltypes.Value{}
	case sqltypes.Timestamp: //{typ: 7},
	case sqltypes.Int64, sqltypes.Uint64: //{typ: 8},
		num, dpos, _ := readUint64(buf, pos)
		pos = dpos
		v = sqltypes.NewUint64(num)
	case sqltypes.Int24: //{typ: 9},
	case sqltypes.Uint24: //{typ: 9, flags: mysqlUnsigned},
	case sqltypes.Date: //{typ: 10, flags: mysqlBinary},
	case sqltypes.Time: //{typ: 11, flags: mysqlBinary},
	case sqltypes.Datetime: //{typ: 12, flags: mysqlBinary},
	case sqltypes.Year: //{typ: 13, flags: mysqlUnsigned},
	case sqltypes.Bit: //{typ: 16, flags: mysqlUnsigned},
	}
	return v, pos, true
}

//A Binary Protocol Resultset Row is made up of the NULL bitmap containing as many bits as
//we have columns in the resultset + 2 and the values for
//columns that are not NULL in the Binary Protocol Value format.
func (c *Conn) writeBinaryRows(result *sqltypes.Result) error {
	for _, row := range result.Rows {
		if err := c.writeBinaryRow(row); err != nil {
			return err
		}
	}
	return nil
}
func (c *Conn) writeBinaryRow(row []sqltypes.Value) error {
	length := 1
	//NULL-bitmap, length: (column-count + 7 + 2) / 8
	//For the Binary Protocol Resultset Row the num-fields and the field-pos need to add a
	//offset of 2. For COM_STMT_EXECUTE this offset is 0.
	//NULL-bitmap-bytes = (num-fields + 7 + offset) / 8
	nullBitmapLen := (len(row) + 7) / 8
	length += nullBitmapLen
	nullBuffs := make([]byte, nullBitmapLen)
	//
	var bigBuff []byte
	for fieldPos, val := range row {
		buff, err := c.getValueBinary(val)
		if err != nil {
			return err
		}
		//if null
		if buff == nil || len(buff) <= 0 {
			bytePos := (fieldPos + 2) / 8
			bitPos := uint8((fieldPos + 2) % 8)
			//doc: https://dev.mysql.com/doc/internals/en/null-bitmap.html
			//nulls[byte_pos] |= 1 << bit_pos
			//nulls[1] |= 1 << 2;
			nullBuffs[bytePos] |= 1 << bitPos
			continue
		}
		//
		length += len(buff)
		bigBuff = append(bigBuff, buff...)
	}
	//
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, 0x00)
	//
	pos += copy(data[pos:], nullBuffs)
	//
	pos += copy(data[pos:], bigBuff)
	if pos != length {
		return fmt.Errorf("internal error packet row: got %v bytes but expected %v", pos, length)
	}
	return c.writeEphemeralPacket(false)
}
func (c *Conn) getValueBinary(val sqltypes.Value) (buff []byte, err error) {
	switch val.Type() {
	case sqltypes.Uint8: //{typ: 1, flags: mysqlUnsigned},
		return val.Raw(), nil
	case sqltypes.Float64: //{typ: 5},//{typ: 4},
		num, err := strconv.ParseFloat(string(val.Raw()), 64)
		if err != nil {
			return nil, err
		}
		bits := math.Float64bits(num)
		buff := make([]byte, 8)
		pos := writeUint64(buff, 0, bits)
		return buff[0:pos], nil
	case sqltypes.Float32: //{typ: 5},//{typ: 4},
		num, err := strconv.ParseFloat(string(val.Raw()), 32)
		if err != nil {
			return nil, err
		}
		bits := math.Float32bits(float32(num))
		buff := make([]byte, 8)
		pos := writeUint32(buff, 0, bits)
		return buff[0:pos], nil
	case sqltypes.Null: //{typ: 6, flags: mysqlBinary},
		return nil, nil
	case sqltypes.Int16, sqltypes.Uint16:
		num, err := strconv.ParseInt(string(val.Raw()), 10, 16)
		if err != nil {
			return nil, err
		}
		buff := make([]byte, 8)
		pos := writeUint16(buff, 0, uint16(num))
		return buff[0:pos], nil
	case sqltypes.Int32, sqltypes.Uint32:
		num, err := strconv.ParseInt(string(val.Raw()), 10, 32)
		if err != nil {
			return nil, err
		}
		buff := make([]byte, 8)
		pos := writeUint32(buff, 0, uint32(num))
		return buff[0:pos], nil
	case sqltypes.Int24, sqltypes.Uint24: //
	case sqltypes.Int64, sqltypes.Uint64: //{typ: 8},
		num, err := strconv.ParseInt(string(val.Raw()), 10, 64)
		if err != nil {
			return nil, err
		}
		buff := make([]byte, 8)
		pos := writeUint64(buff, 0, uint64(num))
		return buff[0:pos], nil
	case sqltypes.Time: //{typ: 11, flags: mysqlBinary},
		return val.Raw(), nil
	case sqltypes.Datetime, sqltypes.Date, sqltypes.Timestamp: //{typ: 12, flags: mysqlBinary},
		/*
		to save space the packet can be compressed:
		if year, month, day, hour, minutes, seconds and micro_seconds are all 0, length is 0 and no other field is sent
		if hour, minutes, seconds and micro_seconds are all 0, length is 4 and no other field is sent
		if micro_seconds is 0, length is 7 and micro_seconds is not sent
		otherwise length is 11

		Fields
		length (1) -- number of bytes following (valid values: 0, 4, 7, 11)
		year (2) -- year
		month (1) -- month
		day (1) -- day
		hour (1) -- hour
		minute (1) -- minutes
		second (1) -- seconds
		micro_second (4) -- micro-seconds
		*/
		//str := string(val.Raw())
		t, err := time.Parse("2006-01-02 15:04:05", string(val.Raw()))
		if err != nil {
			glog.Info(err)
			return []byte{0x00}, nil
		}
		pos := 0
		buff := make([]byte, 12)
		length := 7
		if t.Year()+t.Minute()+t.Day()+t.Hour()+t.Minute()+t.Second() <= 0 {
			return []byte{0x00}, err
		}
		if t.Hour()+t.Minute()+t.Second() <= 0 {
			length = 4
		}
		//
		pos = writeLenEncInt(buff, 0, uint64(length))
		if length >= 4 {
			pos = writeUint16(buff, pos, uint16(t.Year()))
			pos = writeLenEncInt(buff, pos, uint64(t.Month()))
			pos = writeLenEncInt(buff, pos, uint64(t.Day()))
		}
		if length > 4 && length <= 7 {
			pos = writeLenEncInt(buff, pos, uint64(t.Hour()))
			pos = writeLenEncInt(buff, pos, uint64(t.Minute()))
			pos = writeLenEncInt(buff, pos, uint64(t.Second()))
		}
		return buff[0:pos], nil
	case sqltypes.Year: //{typ: 13, flags: mysqlUnsigned},
		return val.Raw(), nil
	case sqltypes.Bit: //{typ: 16, flags: mysqlUnsigned},
		return val.Raw(), nil
	case sqltypes.VarChar, sqltypes.Text, sqltypes.Decimal:
		length := 0
		l := len(val.Raw())
		length += lenEncIntSize(uint64(l)) + l
		vbuff := make([]byte, length)
		pos := writeLenEncInt(vbuff, 0, uint64(l))
		copy(vbuff[pos:], val.Raw())
		return vbuff, nil
	}
	return nil, nil
}
