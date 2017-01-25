To rebuild generated protobuf code, run:

    protoc -I ./ --go_out=plugins=grpc:./ ./httpgrpc.proto

Follow the insturctions here to get a working protoc: https://github.com/golang/protobuf