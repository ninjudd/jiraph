(ns jiraph.masai-layer
  (:refer-clojure :exclude [sync count])
  (:use jiraph.layer
        [clojure.stacktrace :only [print-cause-trace]]
        [retro.core :only [*revision*]]
        [useful.utils :only [if-ns]]
        [useful.seq :only [find-with]])
  (:require [masai.db :as db]
            [cereal.format :as f]
            [cereal.reader :as reader-append-format])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader]
           [clojure.lang LineNumberingPushbackReader]))

(def meta-prefix     "_")
(def internal-prefix "__")
(def property-prefix "___")

(defn- meta-key [id]
  (str meta-prefix id))

(def count-key (str internal-prefix "count"))

(defn- property-key [key]
  (str property-prefix key))

(declare meta-len)
(defn- get-meta [layer key rev]
  (let [db (.db layer)
        mf (.meta-format layer)]
    (if rev
      (let [len (meta-len layer key rev)]
        (when (< 0 len)
          (f/decode mf (db/get db key) 0 len)))
      (f/decode mf (db/get db key)))))

(defn- len
  "The byte length of the node at revision rev."
  [layer id rev]
  (if-not rev
    (db/len (.db layer) id)
    (let [meta (get-meta layer (meta-key id) nil)
          len  (find-with (partial >= rev)
                          (reverse (:rev meta))
                          (reverse (:len meta)))]
      (when (and len (<= 0 len))
        len))))

(defn- meta-len
  "The byte length of the meta node at revision rev."
  [layer key rev]
  (or (when rev
        (let [meta (get-meta layer key nil)]
          ;; Must shift meta lengths by one since they store the length of the previous revision.
          (find-with (partial >= rev)
                     (reverse (cons 0 (:mrev meta)))
                     (cons nil (reverse (:mlen meta))))))
      (db/len (.db layer) key)))

(defn- append-meta! [layer id attrs & [rev]]
  (let [db (.db layer)
        mf (.meta-format layer)
        key (meta-key id)]
    (->> (if rev
           (assoc attrs :mrev rev :mlen (db/len db key))
           attrs)
         (f/encode mf)
         (db/append! db key))))

(defn- set-len! [layer id len]
  (if *revision*
    (append-meta! layer id {:rev *revision* :len len})))

(defn- reset-len! [layer id & [len]]
  ;; Insert a length of -1 to indicate that all lengths before the current one are no longer valid.
  (append-meta! layer id
    (if (and *revision* len)
      {:rev [0 *revision*] :len [-1 len]}
      {:rev 0 :len -1})))

(defn- inc-count! [layer]
  (db/inc! (.db layer) count-key 1))

(defn- dec-count! [layer]
  (db/inc! (:db layer) count-key -1))

(defrecord MasaiLayer [db format meta-format]
  jiraph.layer/Layer

  (open      [layer] (db/open      db))
  (close     [layer] (db/close     db))
  (sync!     [layer] (db/sync!     db))
  (optimize! [layer] (db/optimize! db))

  (node-count [layer]
    (db/inc! db count-key 0))

  (node-ids [layer]
    (remove #(.startsWith % meta-prefix) (db/key-seq db)))

  (fields [layer]
    (f/fields format))

  (fields [layer subfields]
    (f/fields format subfields))

  (node-valid? [layer attrs]
    (try (f/encode format (make-node attrs))
         true
         (catch Exception e)))

  (get-property  [layer key]
    (if-let [bytes (db/get db (property-key key))]
      (read-string (String. bytes))))

  (set-property! [layer key val]
    (let [bytes (.getBytes (pr-str val))]
      (db/put! db (property-key key) bytes)
      val))

  (get-node [layer id]
    (if *revision*
      (when-let [length (len layer id *revision*)]
        (f/decode format (db/get db id) 0 length))
      (f/decode format (db/get db id))))

  (node-exists? [layer id]
    (< 0 (len layer id *revision*)))

  (add-node! [layer id attrs]
    (let [node (make-node attrs)
          data (f/encode format node)]
      (when (db/add! db id data)
        (inc-count! layer)
        (set-len! layer id (alength data))
        (f/decode format data))))

  (set-node! [layer id attrs]
    (let [data (f/encode format (make-node attrs))]
      (db/put! db id data)
      (reset-len! layer id (alength data))
      (f/decode format data)))

  (delete-node! [layer id]
    (let [node (get-node layer id)]
      (when (db/delete! db id)
        (dec-count! layer)
        (reset-len! layer id)
        node)))

  (get-revisions [layer id] (:rev (get-meta layer (meta-key id) *revision*)))
  (get-incoming  [layer id] (:in  (get-meta layer (meta-key id) *revision*)))

  (add-incoming!  [layer id from-id] (append-meta! layer id {:in {from-id true}}  *revision*))
  (drop-incoming! [layer id from-id] (append-meta! layer id {:in {from-id false}} *revision*))

  (truncate! [layer] (db/truncate! db))

  jiraph.layer.Append

  (append-node! [layer id attrs]
    (when-not (empty? attrs)
      (let [len  (db/len db id)
            node (make-node attrs)
            data (f/encode format node)]
        (db/append! db id data)
        (when (= -1 len)
          (inc-count! layer))
        (set-len! layer id (+ (max len 0) (alength data)))
        (f/decode format data))))

  retro.core/WrappedTransactional

  (txn-wrap [layer f]
    (retro.core/wrapped-txn f db))

  retro.core/Revisioned

  (get-revision [layer]
    (get-property layer :rev))

  (set-revision! [layer rev]
    (set-property! layer :rev rev)))

(if-ns (:require protobuf [cereal.protobuf :as protobuf])
       (defn protobuf-make [format]
         (when (instance? cereal.protobuf.ProtobufFormat format)
           (protobuf/make jiraph.Meta$Node)))
       (def protobuf-make (constantly nil)))

(defn make [db & [format meta-format]]
  (let [format (or format (reader-append-format/make))]
    (MasaiLayer.
     db format
     (or meta-format
         (if (instance? cereal.reader.ReaderFormat format)
           (reader-append-format/make {:in #{} :rev [] :len [] :mrev [] :mlen []})
           (protobuf-make format))))))
