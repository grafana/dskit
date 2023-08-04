#!/usr/bin/env bash
# From https://github.com/joe-elliott/cert-exporter/blob/69d3d7230378325a1de4fa313432d3d6ced4a518/test/files/genCerts.sh
certFolder=$1
days=$2

pushd "$certFolder"

# keys
openssl genrsa -out root.key
openssl genrsa -out client.key
openssl genrsa -out server.key

# root cert
openssl req -x509 -new -nodes -key root.key -subj "/C=US/ST=KY/O=Org/CN=root" -sha256 -days "$days" -out root.crt

# csrs
openssl req -new -sha256 -key client.key -subj "/C=US/ST=KY/O=Org/CN=client" -out client.csr
openssl req -new -sha256 -key server.key -subj "/C=US/ST=KY/O=Org/CN=localhost" -out server.csr

openssl x509 -req -in client.csr -CA root.crt -CAkey root.key -CAcreateserial -out client.crt -days "$days" -sha256
openssl x509 -req -in server.csr -CA root.crt -CAkey root.key -CAcreateserial -out server.crt -days "$days" -sha256

popd
