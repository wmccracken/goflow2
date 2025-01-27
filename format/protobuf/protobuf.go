package protobuf

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jnovack/flag"
	"github.com/netsampler/goflow2/format"
	"github.com/netsampler/goflow2/format/common"
)

type ProtobufDriver struct {
	fixedLen bool
}

func (d *ProtobufDriver) Prepare() error {
	common.HashFlag()
	flag.BoolVar(&d.fixedLen, "format.protobuf.fixedlen", false, "Prefix the protobuf with message length")
	return nil
}

func (d *ProtobufDriver) Init(context.Context) error {
	return common.ManualHashInit()
}

func (d *ProtobufDriver) Format(data interface{}) ([]byte, []byte, error) {
	msg, ok := data.(proto.Message)
	if !ok {
		return nil, nil, fmt.Errorf("message is not protobuf")
	}
	key := common.HashProtoLocal(msg)

	if !d.fixedLen {
		b, err := proto.Marshal(msg)
		return []byte(key), b, err
	} else {
		buf := proto.NewBuffer([]byte{})
		err := buf.EncodeMessage(msg)
		return []byte(key), buf.Bytes(), err
	}
}

func init() {
	d := &ProtobufDriver{}
	format.RegisterFormatDriver("pb", d)
}
