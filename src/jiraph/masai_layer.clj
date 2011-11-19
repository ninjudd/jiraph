(ns jiraph.masai-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer LayerMeta NodeMeta
                             ChangeLog node-meta-key]]
        [retro.core   :only [WrappedTransactional Revisioned txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        [useful.utils :only [if-ns adjoin]]
        [useful.seq :only [find-with]])
  (:require [masai.db :as db]
            [cereal.core :as cereal]
            [jiraph.graph :as graph])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader]))



(defrecord MasaiLayer [db revision node-format node-meta-format layer-meta-format]
  Enumerate
  (node-id-seq [this]
    (remove #(.startsWith % "_") (db/key-seq db)))
  (node-seq [this]
    (map #(graph/get-node this %) (node-id-seq this)))

  LayerMeta
  (get-layer-meta [this key]
    (decode layer-meta-format [revision key]
            (db/fetch db (layer-meta-key key))))
  (assoc-layer-meta! [this key val]
    (db/put! db (layer-meta-key key)
             (encode layer-meta-format [revision key] val)))

  NodeMeta
  (get-meta [this id key]
    (decode node-meta-format [revision id key]
            (db/fetch db (node-meta-key id key))))
  (assoc-meta! [this id val]
    (db/put! db (node-meta-key id key)
             (encode node-meta-format [revision id key] val)))

  Basic
  (get-node [this id not-found]
    (if-let [data (db/fetch db id)]
      (decode node-format [revision id] data)
      not-found))
  (assoc-node! [this id attrs]
    (db/put! db id (encode node-format [revision id] attrs)))
  (dissoc-node! [this id]
    (db/delete! db id))

  Optimized
  (query-fn [layer keyseq f] nil)
  (update-fn [layer keyseq f]
    (when (= f adjoin)
      (fn [attrs]
        (db/append! db (node-meta-key id key)
                    (encode node-meta-format [revision id key] (first args)))
        )))

  Layer
  (open [layer]
    (db/open db))
  (close [layer]
    (db/close db))
  (sync! [layer]
    (db/sync! db))
  (optimize! [layer]
    (db/optimize! db))
  (truncate! [layer]
    (db/truncate! db))

  ;; Schema
  ;; (fields [layer]
  ;;   (f/fields format))
  ;; (fields [layer subfields]
  ;;   (f/fields format subfields))
  ;; (node-valid? [layer attrs]
  ;;   (try (f/encode format (make-node attrs))
  ;;        true
  ;;        (catch Exception e)))

  ChangeLog
  (get-revisions [this id]
    (:revisions (graph/get-node this id)))

  WrappedTransactional
  (txn-wrap [this f]
    ;; todo *skip-writes* for past revisions
    (txn-wrap f db))

  Revisioned
  (at-revision [this rev]
    (assoc this :revision rev))
  (current-revision [this]
    revision))

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
