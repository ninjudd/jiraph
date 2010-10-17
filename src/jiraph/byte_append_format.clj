(ns jiraph.byte-append-format
  (:refer-clojure :exclude [load]))

(defprotocol ByteAppendFormat "Byte-layer serialization format"
  (load   [format data] [format data len])
  (dump   [format node])
  (fields [format]))