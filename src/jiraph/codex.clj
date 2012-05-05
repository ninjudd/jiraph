(ns jiraph.codex
  "Jiraph codexes are a slight generalization of gloss codecs.
A codec encodes to and from a sequence of ByteBuffers; but because masai expects byte[] arguments,
we'd like it to be possible to avoid going through ByteBuffers. So, a codex transforms from an
object<=>byte[].

The default implementation of the codex protocol is to use gloss codecs by wrapping and unwrapping
the byte arrays into ByteBuffer sequences. There is an additional implementation for maps, which
looks in the :read and :write keys for encode and decode functions going directly from objects to
byte-arrays. For performance, there is also a Codex record holding :read and :write keys that
behaves similarly."
  (:use [io.core :only [bufseq->bytes]])
  (:require [gloss.io :as gloss])
  (:import java.nio.ByteBuffer))

(defprotocol ByteCoder
  (decode [this byte-array] "Read an object from this byte-array.")
  (encode [this obj] "Create a byte-array encoding this object."))

(defrecord Codex [read write]
  ByteCoder
  (decode [this byte-array]
    (read byte-array))
  (encode [this obj]
    (write obj)))

(defn wrap
  "Return a codex wrapping the existing one by calling pre-encode before encoding, and post-decode
after decoding.

The generalized version of gloss/compile-frame."
  [codex pre-encode post-decode]
  (Codex. (fn [bytes]
            (post-decode (decode codex bytes)))
          (fn [obj]
            (encode codex (pre-encode obj)))))

(extend-protocol ByteCoder
  clojure.lang.IPersistentMap
  (decode [this byte-array]
    ((:read this) byte-array))
  (encode [this obj]
    ((:write this) obj))

  Object ;; hoping they implement gloss Reader/Writer
  (decode [this byte-array]
    (gloss/decode this [(ByteBuffer/wrap byte-array)]))
  (encode [this obj]
    (bufseq->bytes (gloss/encode this obj))))
