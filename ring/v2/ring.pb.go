// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.1
// 	protoc        v3.6.1
// source: ring/v2/ring.proto

package v2

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type InstanceState int32

const (
	InstanceState_ACTIVE  InstanceState = 0
	InstanceState_LEAVING InstanceState = 1
	InstanceState_PENDING InstanceState = 2
	InstanceState_JOINING InstanceState = 3
	// This state is only used by gossiping code to distribute information about
	// instances that have been removed from the ring. Ring users should not use it directly.
	InstanceState_LEFT InstanceState = 4
)

// Enum value maps for InstanceState.
var (
	InstanceState_name = map[int32]string{
		0: "InstanceState_ACTIVE",
		1: "InstanceState_LEAVING",
		2: "InstanceState_PENDING",
		3: "InstanceState_JOINING",
		4: "InstanceState_LEFT",
	}
	InstanceState_value = map[string]int32{
		"InstanceState_ACTIVE":  0,
		"InstanceState_LEAVING": 1,
		"InstanceState_PENDING": 2,
		"InstanceState_JOINING": 3,
		"InstanceState_LEFT":    4,
	}
)

func (x InstanceState) Enum() *InstanceState {
	p := new(InstanceState)
	*p = x
	return p
}

func (x InstanceState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (InstanceState) Descriptor() protoreflect.EnumDescriptor {
	return file_ring_v2_ring_proto_enumTypes[0].Descriptor()
}

func (InstanceState) Type() protoreflect.EnumType {
	return &file_ring_v2_ring_proto_enumTypes[0]
}

func (x InstanceState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use InstanceState.Descriptor instead.
func (InstanceState) EnumDescriptor() ([]byte, []int) {
	return file_ring_v2_ring_proto_rawDescGZIP(), []int{0}
}

// Desc is the top-level type used to model a ring, containing information for individual instances.
type Desc struct {
	state         protoimpl.MessageState   `protogen:"open.v1"`
	Ingesters     map[string]*InstanceDesc `protobuf:"bytes,1,rep,name=ingesters,proto3" json:"ingesters,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Desc) Reset() {
	*x = Desc{}
	mi := &file_ring_v2_ring_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Desc) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Desc) ProtoMessage() {}

func (x *Desc) ProtoReflect() protoreflect.Message {
	mi := &file_ring_v2_ring_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Desc.ProtoReflect.Descriptor instead.
func (*Desc) Descriptor() ([]byte, []int) {
	return file_ring_v2_ring_proto_rawDescGZIP(), []int{0}
}

func (x *Desc) GetIngesters() map[string]*InstanceDesc {
	if x != nil {
		return x.Ingesters
	}
	return nil
}

// InstanceDesc is the top-level type used to model per-instance information in a ring.
type InstanceDesc struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	Addr  string                 `protobuf:"bytes,1,opt,name=addr,proto3" json:"addr,omitempty"`
	// Unix timestamp (with seconds precision) of the last heartbeat sent
	// by this instance.
	Timestamp int64         `protobuf:"varint,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	State     InstanceState `protobuf:"varint,3,opt,name=state,proto3,enum=v2.InstanceState" json:"state,omitempty"`
	Tokens    []uint32      `protobuf:"varint,6,rep,packed,name=tokens,proto3" json:"tokens,omitempty"`
	Zone      string        `protobuf:"bytes,7,opt,name=zone,proto3" json:"zone,omitempty"`
	// Unix timestamp (with seconds precision) of when the instance has been registered
	// to the ring. This field has not been called "joined_timestamp" intentionally, in order
	// to not introduce any misunderstanding with the instance's "joining" state.
	//
	// This field is used to find out subset of instances that could have possibly owned a
	// specific token in the past. Because of this, it's important that either this timestamp
	// is set to the real time the instance has been registered to the ring or it's left
	// 0 (which means unknown).
	//
	// When an instance is already registered in the ring with a value of 0 it's NOT safe to
	// update the timestamp to "now" because it would break the contract, given the instance
	// was already registered before "now". If unknown (0), it should be left as is, and the
	// code will properly deal with that.
	RegisteredTimestamp int64 `protobuf:"varint,8,opt,name=registered_timestamp,json=registeredTimestamp,proto3" json:"registered_timestamp,omitempty"`
	// ID of the instance. This value is the same as the key in the ingesters map in Desc.
	Id string `protobuf:"bytes,9,opt,name=id,proto3" json:"id,omitempty"`
	// Unix timestamp (with seconds precision) of when the read_only flag was updated. This
	// is used to find other instances that could have possibly owned a specific token in
	// the past on the write path, due to *this* instance being read-only. This value should
	// only increase.
	ReadOnlyUpdatedTimestamp int64 `protobuf:"varint,10,opt,name=read_only_updated_timestamp,json=readOnlyUpdatedTimestamp,proto3" json:"read_only_updated_timestamp,omitempty"`
	// Indicates whether this instance is read only.
	// Read-only instances go through standard state changes, and special handling is applied to them
	// during shuffle shards.
	ReadOnly      bool `protobuf:"varint,11,opt,name=read_only,json=readOnly,proto3" json:"read_only,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InstanceDesc) Reset() {
	*x = InstanceDesc{}
	mi := &file_ring_v2_ring_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InstanceDesc) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InstanceDesc) ProtoMessage() {}

func (x *InstanceDesc) ProtoReflect() protoreflect.Message {
	mi := &file_ring_v2_ring_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InstanceDesc.ProtoReflect.Descriptor instead.
func (*InstanceDesc) Descriptor() ([]byte, []int) {
	return file_ring_v2_ring_proto_rawDescGZIP(), []int{1}
}

func (x *InstanceDesc) GetAddr() string {
	if x != nil {
		return x.Addr
	}
	return ""
}

func (x *InstanceDesc) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *InstanceDesc) GetState() InstanceState {
	if x != nil {
		return x.State
	}
	return InstanceState_ACTIVE
}

func (x *InstanceDesc) GetTokens() []uint32 {
	if x != nil {
		return x.Tokens
	}
	return nil
}

func (x *InstanceDesc) GetZone() string {
	if x != nil {
		return x.Zone
	}
	return ""
}

func (x *InstanceDesc) GetRegisteredTimestamp() int64 {
	if x != nil {
		return x.RegisteredTimestamp
	}
	return 0
}

func (x *InstanceDesc) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *InstanceDesc) GetReadOnlyUpdatedTimestamp() int64 {
	if x != nil {
		return x.ReadOnlyUpdatedTimestamp
	}
	return 0
}

func (x *InstanceDesc) GetReadOnly() bool {
	if x != nil {
		return x.ReadOnly
	}
	return false
}

var File_ring_v2_ring_proto protoreflect.FileDescriptor

var file_ring_v2_ring_proto_rawDesc = []byte{
	0x0a, 0x12, 0x72, 0x69, 0x6e, 0x67, 0x2f, 0x76, 0x32, 0x2f, 0x72, 0x69, 0x6e, 0x67, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x12, 0x02, 0x76, 0x32, 0x22, 0x93, 0x01, 0x0a, 0x04, 0x44, 0x65, 0x73,
	0x63, 0x12, 0x35, 0x0a, 0x09, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x65, 0x72, 0x73, 0x18, 0x01,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x76, 0x32, 0x2e, 0x44, 0x65, 0x73, 0x63, 0x2e, 0x49,
	0x6e, 0x67, 0x65, 0x73, 0x74, 0x65, 0x72, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x09, 0x69,
	0x6e, 0x67, 0x65, 0x73, 0x74, 0x65, 0x72, 0x73, 0x1a, 0x4e, 0x0a, 0x0e, 0x49, 0x6e, 0x67, 0x65,
	0x73, 0x74, 0x65, 0x72, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65,
	0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x26, 0x0a, 0x05,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x76, 0x32,
	0x2e, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x44, 0x65, 0x73, 0x63, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x4a, 0x04, 0x08, 0x02, 0x10, 0x03, 0x22, 0xc0,
	0x02, 0x0a, 0x0c, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x44, 0x65, 0x73, 0x63, 0x12,
	0x12, 0x0a, 0x04, 0x61, 0x64, 0x64, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x61,
	0x64, 0x64, 0x72, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x12, 0x27, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x11, 0x2e, 0x76, 0x32, 0x2e, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x74, 0x6f,
	0x6b, 0x65, 0x6e, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0d, 0x52, 0x06, 0x74, 0x6f, 0x6b, 0x65,
	0x6e, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x7a, 0x6f, 0x6e, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x7a, 0x6f, 0x6e, 0x65, 0x12, 0x31, 0x0a, 0x14, 0x72, 0x65, 0x67, 0x69, 0x73, 0x74,
	0x65, 0x72, 0x65, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x08,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x13, 0x72, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x65, 0x64,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18,
	0x09, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x3d, 0x0a, 0x1b, 0x72, 0x65, 0x61,
	0x64, 0x5f, 0x6f, 0x6e, 0x6c, 0x79, 0x5f, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x03, 0x52, 0x18,
	0x72, 0x65, 0x61, 0x64, 0x4f, 0x6e, 0x6c, 0x79, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x54,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1b, 0x0a, 0x09, 0x72, 0x65, 0x61, 0x64,
	0x5f, 0x6f, 0x6e, 0x6c, 0x79, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x72, 0x65, 0x61,
	0x64, 0x4f, 0x6e, 0x6c, 0x79, 0x4a, 0x04, 0x08, 0x04, 0x10, 0x05, 0x4a, 0x04, 0x08, 0x05, 0x10,
	0x06, 0x2a, 0x4c, 0x0a, 0x0d, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x12, 0x0a, 0x0a, 0x06, 0x41, 0x43, 0x54, 0x49, 0x56, 0x45, 0x10, 0x00, 0x12, 0x0b,
	0x0a, 0x07, 0x4c, 0x45, 0x41, 0x56, 0x49, 0x4e, 0x47, 0x10, 0x01, 0x12, 0x0b, 0x0a, 0x07, 0x50,
	0x45, 0x4e, 0x44, 0x49, 0x4e, 0x47, 0x10, 0x02, 0x12, 0x0b, 0x0a, 0x07, 0x4a, 0x4f, 0x49, 0x4e,
	0x49, 0x4e, 0x47, 0x10, 0x03, 0x12, 0x08, 0x0a, 0x04, 0x4c, 0x45, 0x46, 0x54, 0x10, 0x04, 0x42,
	0x27, 0x5a, 0x25, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x72,
	0x61, 0x66, 0x61, 0x6e, 0x61, 0x2f, 0x63, 0x6f, 0x72, 0x74, 0x65, 0x78, 0x2f, 0x70, 0x6b, 0x67,
	0x2f, 0x72, 0x69, 0x6e, 0x67, 0x2f, 0x76, 0x32, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_ring_v2_ring_proto_rawDescOnce sync.Once
	file_ring_v2_ring_proto_rawDescData = file_ring_v2_ring_proto_rawDesc
)

func file_ring_v2_ring_proto_rawDescGZIP() []byte {
	file_ring_v2_ring_proto_rawDescOnce.Do(func() {
		file_ring_v2_ring_proto_rawDescData = protoimpl.X.CompressGZIP(file_ring_v2_ring_proto_rawDescData)
	})
	return file_ring_v2_ring_proto_rawDescData
}

var file_ring_v2_ring_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_ring_v2_ring_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_ring_v2_ring_proto_goTypes = []any{
	(InstanceState)(0),   // 0: v2.InstanceState
	(*Desc)(nil),         // 1: v2.Desc
	(*InstanceDesc)(nil), // 2: v2.InstanceDesc
	nil,                  // 3: v2.Desc.IngestersEntry
}
var file_ring_v2_ring_proto_depIdxs = []int32{
	3, // 0: v2.Desc.ingesters:type_name -> v2.Desc.IngestersEntry
	0, // 1: v2.InstanceDesc.state:type_name -> v2.InstanceState
	2, // 2: v2.Desc.IngestersEntry.value:type_name -> v2.InstanceDesc
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_ring_v2_ring_proto_init() }
func file_ring_v2_ring_proto_init() {
	if File_ring_v2_ring_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_ring_v2_ring_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_ring_v2_ring_proto_goTypes,
		DependencyIndexes: file_ring_v2_ring_proto_depIdxs,
		EnumInfos:         file_ring_v2_ring_proto_enumTypes,
		MessageInfos:      file_ring_v2_ring_proto_msgTypes,
	}.Build()
	File_ring_v2_ring_proto = out.File
	file_ring_v2_ring_proto_rawDesc = nil
	file_ring_v2_ring_proto_goTypes = nil
	file_ring_v2_ring_proto_depIdxs = nil
}
