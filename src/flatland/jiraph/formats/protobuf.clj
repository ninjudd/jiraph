(ns flatland.jiraph.formats.protobuf
  (:use [flatland.useful.seq :only [lazy-loop]]
        [flatland.useful.map :only [keyed update]]
        [flatland.useful.utils :only [adjoin]]
        [flatland.useful.experimental :only [lift-meta]]
        [flatland.protobuf.core :only [protodef protobuf? protobuf protobuf-load]])
  (:require [flatland.protobuf.codec :as protobuf]
            [flatland.jiraph.codex :as codex]
            [flatland.schematic.core :as schema])
  (:import java.nio.ByteBuffer
           com.google.protobuf.CodedOutputStream
           flatland.protobuf.PersistentProtocolBufferMap
           flatland.protobuf.PersistentProtocolBufferMap$Def))

(defn- proto-format* [proto]
  (let [schema (-> (protobuf/codec-schema proto)
                   (schema/dissoc-fields :revisions))
        codec (protobuf/protobuf-codec proto)
        reduce-fn adjoin]
    (keyed [codec schema reduce-fn])))

(defn basic-protobuf-format
  "A protobuf format which keeps track of revisions by making them part of the encoded protobuf
   data. Not intended to be used in append-only mode; instead, the :revisions key is manually
   curated to contain both its old values and the new one."
  [proto]
  (let [format (proto-format* proto)
        format (-> format
                   (assoc :revisions ;; set the revision-reading codec
                     (codex/wrap (:codec format) identity :revisions))
                   (update :codec codex/wrap identity #(lift-meta % [:revisions])))]
    (fn [{:keys [revision]}]
      (if-not revision
        format
        (update format :codec codex/wrap
                #(update % :revisions conj revision)
                identity)))))

;; the header for each revision-chunk is:
;; 1) the magic byte 0x7a, which means "field #15, a length-delimited struct". By using 15 here, we
;;    are effectively reserving it: no revisioned protobuf can use field #15 for anything else.
;;    protobuf skips fields it doesn't know about, and we're using this to skip the header.
;; 2) The byte 0x0c (decimal 12), telling protobuf the struct to skip is 12 bytes long
;; 3) 8 bytes of revision number. If the revision number is negative, it indicates that a "codec
;;    reset" was performed at that revision, meaning that all previous data should be
;;    discarded. Then, negate the number to see the actual revision.
;; 4) 4 bytes indicating the byte-length of the data written for this revision.
;;
;; So, to read the data at a given revision, we must skip through the encoded bytes, reading each
;; header, and find the start and end offset to ask protobuf to decode. Start will be either 0, or
;; the offset of the codec-reset written most recently before the requested revision; end will be
;; either the end of the encoded data, or the offset at which data for that revision ends.
(let [field-number 15
      wire-type 2
      fieldno-byte (unchecked-byte (bit-or (bit-shift-left field-number 3)
                                           wire-type))
      content-length 12
      length-byte (byte content-length)
      magic-header (short (bit-or (bit-shift-left fieldno-byte 8)
                                  length-byte))
      total-header-size (+ 2 content-length)]

  (defn defines-field?
    "Determines whether the protobuf defines a field with the specified number."
    [^PersistentProtocolBufferMap$Def proto, field-num]
    (-> proto
        (.type)
        (.findFieldByNumber field-num)))

  (defn verify-field-available
    "Throw an exception if the protobuf defines a field with the number of our reserved header."
    [proto]
    (when (defines-field? proto field-number)
      (throw (IllegalArgumentException.
              (format "Can't encode protobuf that defines field %d, as we reserve it to encode revisions."
                      field-number)))))

  (defn revision-offsets
    "Return a lazy sequence of tuples: [revision-number, start-offset, end-offset, is-reset].
     They are in descending order by revision number: the latest revision comes first."
    [^ByteBuffer buffer]
    (.position buffer (.limit buffer))  ; start at the end and read backwards
    (lazy-loop []
      (let [pos (.position buffer)
            end-offset (- pos total-header-size)]
        (when-not (neg? end-offset)     ; more data to read
          (.position buffer end-offset) ; skip backwards over it
          (assert (= magic-header (.getShort buffer)))
          (let [revision (.getLong buffer)
                length (.getInt buffer)
                [revision reset?] (if (neg? revision)
                                    [(- revision) true]
                                    [revision false])
                start-offset (- end-offset length)]
            (.position buffer start-offset) ;; skip backwards over the data for that revision
            (cons [revision start-offset end-offset reset?]
                  (lazy-recur)))))))

  (defn offsets-for-revision
    "Given some offsets (as produced by revision-offsets), returns only those offsets which are
     necessary to read the data at some particular revision. Basically, this entails dropping any
     offsets which were written after that revision, and any that were written before a later
     reset."
    [buffer-offsets read-rev]
    (lazy-loop [offsets (seq buffer-offsets)]
      (when offsets
        (let [[revision begin end reset? :as offset] (first offsets)
              more (next offsets)]
          (cond (> revision read-rev) (recur more) ;; in the future: pretend it doesn't exist
                reset? [offset]
                :else (cons offset (lazy-recur more)))))))

  (defn revisions
    "Get just the revision numbers from a sequence of revision offsets."
    [offsets]
    (map first offsets))

  (defn slice
    "Given a sequence of buffer offsets, returns a [start length] pair describing
     what piece of the buffer must be read to obtain the requested data."
    [offsets]
    (if (empty? offsets)
      [0 0]
      (let [[_ _ end] (first offsets)
            [_ begin] (last offsets)]
        [begin (- end begin)])))

  (defn revisioned-writer
    "Given a protodef, a revision, and whether to reset, return the write half of a jiraph codex
     (ie, a function from a node to a byte-array)."
    [proto revision reset?]
    (fn [node]
      (let [^PersistentProtocolBufferMap val (if (protobuf? node)
                                               node
                                               (protobuf proto node))
            message (.message val)
            len (.getSerializedSize message)
            ary (byte-array (+ len total-header-size))
            out (CodedOutputStream/newInstance ary 0 len)
            buf (ByteBuffer/wrap ary)]
        (.writeTo message out)
        (.position buf len)
        (.putShort buf magic-header)
        (.putLong buf (if reset? (- revision), revision))
        (.putInt buf len)
        ary)))

  (defn revisioned-reader
    "Given a revision, and a function to call with some bytes and the revision-offsets for that
     revision, return the read half of a jiraph codex (ie, a function from a byte-array to a node)."
    [revision use-offsets]
    (fn [^bytes bytes]
      (let [buf (ByteBuffer/wrap bytes)
            bounds (offsets-for-revision (revision-offsets buf)
                                         (or revision Double/POSITIVE_INFINITY))]
        (use-offsets bytes bounds))))

  (defn protobuf-format
    "Produce a jiraph format from a protobuf. The format will *only* support writing with a
    revision, and reading data produced by its writer: it cannot read a simple protobuf-dump,
    because that would lack the revision metadata it expects to find. For a non-revisioned format,
    use basic-protobuf-format instead. However, data written by this format can be read with
    protobuf-load, if you only want to get the latest information: the revision metadata is hidden
    from protobuf."
    [proto]
    (let [proto (protodef proto)
          proto-format (proto-format* proto)]
      (verify-field-available proto)
      (fn [{:keys [revision]}]
        (letfn [(reader [use-offsets]
                  {:read (revisioned-reader revision use-offsets)})
                (writer [reset?]
                  {:write (revisioned-writer proto revision reset?)})]
          (assoc proto-format
            :codec (merge (writer false)
                          (reader (fn [bytes bounds]
                                    (-> (apply protobuf-load proto bytes (slice bounds))
                                        (vary-meta assoc :revisions (revisions bounds))))))
            :reset (writer true) ;; no reader, so will break if anyone reads with it
            :revisions (reader (fn [bytes bounds]
                                 (revisions bounds)))))))))
