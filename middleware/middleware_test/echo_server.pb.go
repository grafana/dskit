// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: echo_server.proto

package middleware_test

import (
	bytes "bytes"
	context "context"
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	io "io"
	math "math"
	math_bits "math/bits"
	reflect "reflect"
	strings "strings"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type Msg struct {
	Body []byte `protobuf:"bytes,1,opt,name=body,proto3" json:"body,omitempty"`
}

func (m *Msg) Reset()      { *m = Msg{} }
func (*Msg) ProtoMessage() {}
func (*Msg) Descriptor() ([]byte, []int) {
	return fileDescriptor_f24ecbb5972b7a26, []int{0}
}
func (m *Msg) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Msg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Msg.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Msg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Msg.Merge(m, src)
}
func (m *Msg) XXX_Size() int {
	return m.Size()
}
func (m *Msg) XXX_DiscardUnknown() {
	xxx_messageInfo_Msg.DiscardUnknown(m)
}

var xxx_messageInfo_Msg proto.InternalMessageInfo

func (m *Msg) GetBody() []byte {
	if m != nil {
		return m.Body
	}
	return nil
}

func init() {
	proto.RegisterType((*Msg)(nil), "middleware.Msg")
}

func init() { proto.RegisterFile("echo_server.proto", fileDescriptor_f24ecbb5972b7a26) }

var fileDescriptor_f24ecbb5972b7a26 = []byte{
	// 190 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x4c, 0x4d, 0xce, 0xc8,
	0x8f, 0x2f, 0x4e, 0x2d, 0x2a, 0x4b, 0x2d, 0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0xca,
	0xcd, 0x4c, 0x49, 0xc9, 0x49, 0x2d, 0x4f, 0x2c, 0x4a, 0x55, 0x92, 0xe4, 0x62, 0xf6, 0x2d, 0x4e,
	0x17, 0x12, 0xe2, 0x62, 0x49, 0xca, 0x4f, 0xa9, 0x94, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x09, 0x02,
	0xb3, 0x8d, 0xec, 0xb9, 0xb8, 0x5c, 0x93, 0x33, 0xf2, 0x83, 0xc1, 0x5a, 0x85, 0x0c, 0xb9, 0xd8,
	0x03, 0x8a, 0xf2, 0x93, 0x53, 0x8b, 0x8b, 0x85, 0xf8, 0xf5, 0x10, 0x06, 0xe8, 0xf9, 0x16, 0xa7,
	0x4b, 0xa1, 0x0b, 0x28, 0x31, 0x68, 0x30, 0x1a, 0x30, 0x3a, 0xb9, 0x5e, 0x78, 0x28, 0xc7, 0x70,
	0xe3, 0xa1, 0x1c, 0xc3, 0x87, 0x87, 0x72, 0x8c, 0x0d, 0x8f, 0xe4, 0x18, 0x57, 0x3c, 0x92, 0x63,
	0x3c, 0xf1, 0x48, 0x8e, 0xf1, 0xc2, 0x23, 0x39, 0xc6, 0x07, 0x8f, 0xe4, 0x18, 0x5f, 0x3c, 0x92,
	0x63, 0xf8, 0xf0, 0x48, 0x8e, 0x71, 0xc2, 0x63, 0x39, 0x86, 0x0b, 0x8f, 0xe5, 0x18, 0x6e, 0x3c,
	0x96, 0x63, 0x88, 0xe2, 0x47, 0x98, 0x15, 0x5f, 0x92, 0x5a, 0x5c, 0x92, 0xc4, 0x06, 0x76, 0xb5,
	0x31, 0x20, 0x00, 0x00, 0xff, 0xff, 0xce, 0xf1, 0x2d, 0xa4, 0xca, 0x00, 0x00, 0x00,
}

func (this *Msg) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*Msg)
	if !ok {
		that2, ok := that.(Msg)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if !bytes.Equal(this.Body, that1.Body) {
		return false
	}
	return true
}
func (this *Msg) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 5)
	s = append(s, "&middleware_test.Msg{")
	s = append(s, "Body: "+fmt.Sprintf("%#v", this.Body)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func valueToGoStringEchoServer(v interface{}, typ string) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("func(v %v) *%v { return &v } ( %#v )", typ, typ, pv)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// EchoServerClient is the client API for EchoServer service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type EchoServerClient interface {
	Process(ctx context.Context, opts ...grpc.CallOption) (EchoServer_ProcessClient, error)
}

type echoServerClient struct {
	cc *grpc.ClientConn
}

func NewEchoServerClient(cc *grpc.ClientConn) EchoServerClient {
	return &echoServerClient{cc}
}

func (c *echoServerClient) Process(ctx context.Context, opts ...grpc.CallOption) (EchoServer_ProcessClient, error) {
	stream, err := c.cc.NewStream(ctx, &_EchoServer_serviceDesc.Streams[0], "/middleware.EchoServer/Process", opts...)
	if err != nil {
		return nil, err
	}
	x := &echoServerProcessClient{stream}
	return x, nil
}

type EchoServer_ProcessClient interface {
	Send(*Msg) error
	Recv() (*Msg, error)
	grpc.ClientStream
}

type echoServerProcessClient struct {
	grpc.ClientStream
}

func (x *echoServerProcessClient) Send(m *Msg) error {
	return x.ClientStream.SendMsg(m)
}

func (x *echoServerProcessClient) Recv() (*Msg, error) {
	m := new(Msg)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// EchoServerServer is the server API for EchoServer service.
type EchoServerServer interface {
	Process(EchoServer_ProcessServer) error
}

// UnimplementedEchoServerServer can be embedded to have forward compatible implementations.
type UnimplementedEchoServerServer struct {
}

func (*UnimplementedEchoServerServer) Process(srv EchoServer_ProcessServer) error {
	return status.Errorf(codes.Unimplemented, "method Process not implemented")
}

func RegisterEchoServerServer(s *grpc.Server, srv EchoServerServer) {
	s.RegisterService(&_EchoServer_serviceDesc, srv)
}

func _EchoServer_Process_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(EchoServerServer).Process(&echoServerProcessServer{stream})
}

type EchoServer_ProcessServer interface {
	Send(*Msg) error
	Recv() (*Msg, error)
	grpc.ServerStream
}

type echoServerProcessServer struct {
	grpc.ServerStream
}

func (x *echoServerProcessServer) Send(m *Msg) error {
	return x.ServerStream.SendMsg(m)
}

func (x *echoServerProcessServer) Recv() (*Msg, error) {
	m := new(Msg)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _EchoServer_serviceDesc = grpc.ServiceDesc{
	ServiceName: "middleware.EchoServer",
	HandlerType: (*EchoServerServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Process",
			Handler:       _EchoServer_Process_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "echo_server.proto",
}

func (m *Msg) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Msg) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Msg) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Body) > 0 {
		i -= len(m.Body)
		copy(dAtA[i:], m.Body)
		i = encodeVarintEchoServer(dAtA, i, uint64(len(m.Body)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintEchoServer(dAtA []byte, offset int, v uint64) int {
	offset -= sovEchoServer(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Msg) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Body)
	if l > 0 {
		n += 1 + l + sovEchoServer(uint64(l))
	}
	return n
}

func sovEchoServer(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozEchoServer(x uint64) (n int) {
	return sovEchoServer(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (this *Msg) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&Msg{`,
		`Body:` + fmt.Sprintf("%v", this.Body) + `,`,
		`}`,
	}, "")
	return s
}
func valueToStringEchoServer(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("*%v", pv)
}
func (m *Msg) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowEchoServer
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Msg: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Msg: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Body", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEchoServer
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthEchoServer
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthEchoServer
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Body = append(m.Body[:0], dAtA[iNdEx:postIndex]...)
			if m.Body == nil {
				m.Body = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipEchoServer(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthEchoServer
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipEchoServer(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowEchoServer
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowEchoServer
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowEchoServer
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthEchoServer
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupEchoServer
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthEchoServer
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthEchoServer        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowEchoServer          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupEchoServer = fmt.Errorf("proto: unexpected end of group")
)
