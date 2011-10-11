(ns jiraph.masai-layer
  (:use [jiraph.layer :only [Enumerate Counted Optimized Basic Layer LayerMeta]]
        [retro.core   :only [WrappedTransactional Revisioned]]
        [clojure.stacktrace :only [print-cause-trace]]
        [retro.core :only [*revision*]]
        [useful.utils :only [if-ns]]
        [useful.seq :only [find-with]])
  (:require [masai.db :as db]
            [cereal.format :as f]
            [cereal.reader :as reader-append-format])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader]
           [clojure.lang LineNumberingPushbackReader]))

(defn- layer-meta-key [key] (str "_" key))
(defn- node-meta-key [id key] (str "_" id "_" key))

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

(defrecord MasaiLayer [db format node-meta-format layer-meta-format]
  Enumerate
  (node-ids-seq [this]
    (remove #(.startsWith % "_") (db/key-seq db)))
  (node-seq [this]
    (map #(get-node layer % nil) (node-id-seq this)))

  Counted
  (node-count [this]
    (or (db/fetch db count-key) 0))

  LayerMeta
  (get-layer-meta [this key]
    (f/decode layer-meta-format
              (db/get db (layer-meta-key key))))
  (assoc-layer-meta! [this key val]
    (db/put! db (layer-meta-key key)
             (f/encode layer-meta-format val)))

  NodeMeta
  (get-meta [this id key]
    (f/decode node-meta-format
              (db/get db (node-meta-key id key))))
  (assoc-meta! [this id val]
    (db/put! db (node-meta-key id key)
             (f/encode node-meta-format val)))

  Basic
  (get-node [this id]
    (if *revision*
      (when-let [length (len this id *revision*)]
        (f/decode format (db/get db id) 0 length))
      (f/decode format (db/get db id))))
  (assoc-node! [this id attrs]
    (let [data (f/encode format (make-node attrs))]
      (db/put! db id data)
      (reset-len! this id (alength data))
      (f/decode format data)))
  (dissoc-node! [this id]
    (let [node (get-node this id)]
      (when (db/delete! db id)
        (dec-count! layer)
        (reset-len! layer id)
        node)))

  Layer
  (open [layer]
    (db/open db))
  (close [layer]
    (db/close db))
  (sync! [layer]
    (db/sync! db))
  (optimize! [layer]
    (db/optimize! db))

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




  (node-exists? [layer id]
    (< 0 (len layer id *revision*)))

  (add-node! [layer id attrs]
    (let [node (make-node attrs)
          data (f/encode format node)]
      (when (db/add! db id data)
        (inc-count! layer)
        (set-len! layer id (alength data))
        (f/decode format data))))





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

(if-ns (:require protobuf.core
                 [cereal.protobuf :as protobuf])
       (defn- make-protobuf [format]
         (when (instance? cereal.protobuf.ProtobufFormat format)
           (protobuf/make jiraph.Meta$Node)))
       (defn- make-protobuf [format]
         nil))

(if-ns (:require [masai.tokyo :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

(defn make [db & [format meta-format]]
  (let [format (or format (reader-append-format/make))]
    (MasaiLayer.
     (make-db db) format
     (or meta-format
         (if (instance? cereal.reader.ReaderFormat format)
           (reader-append-format/make {:in #{} :rev [] :len [] :mrev [] :mlen []})
           (make-protobuf format))))))
