(ns jiraph.protobuf-append-format
  (:use protobuf)
  (:require jiraph.byte-append-format))

(deftype ProtobufAppendFormat [proto]
  jiraph.byte-append-format/ByteAppendFormat
  
  (load [format data]
    (if data (protobuf-load proto data)))
  
  (load [format data len]
    (if data (protobuf-load proto data len)))
  
  (dump [format node]
    (protobuf-dump
     (if (protobuf? node) node (protobuf proto node))))

  (fields [format]
    (map first (protofields proto))))

(defn make [proto]
  (ProtobufAppendFormat. (protodef proto)))