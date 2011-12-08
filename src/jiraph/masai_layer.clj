(ns jiraph.masai-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer ChangeLog Meta Preferences
                             node-id-seq meta-key meta-key?] :as layer]
        [retro.core   :only [WrappedTransactional Revisioned txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        [useful.utils :only [if-ns adjoin]]
        [useful.seq :only [find-with]]
        [useful.fn :only [as-fn]]
        [useful.datatypes :only [assoc-record]]
        [gloss.io :only [encode decode]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [jiraph.graph :as graph]
            [jiraph.codecs.cereal :as cereal])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader]
           [java.nio ByteBuffer]))

(defn- format-for [layer node-id]
  (get layer (cond (= node-id (meta-key layer "_layer")) :layer-meta-format
                   (meta-key? layer node-id) :node-meta-format
                   :else :node-format)))

;; drop leading _ - NB must undo the meta-key impl in MasaiLayer
(defn- main-node-id [meta-id]
  {:pre [(= "_" (first meta-id))]}
  (subs meta-id 1))

(defn- raw-get-node [layer id not-found]
  (if-let [data (db/fetch (:db layer) id)]
    (decode ((format-for layer id) {:revision (:revision layer)})
            [(ByteBuffer/wrap data)])
    not-found))

(defrecord MasaiLayer [db revision node-format node-meta-format layer-meta-format]
  Meta
  (meta-key [this k]
    (str "_" k))
  (meta-key? [this k]
    (.startsWith ^String k "_"))

  Enumerate
  (node-id-seq [this]
    (remove #(meta-key? this %) (db/key-seq db)))
  (node-seq [this]
    (map #(graph/get-node this %) (node-id-seq this)))

  Basic
  (get-node [this id not-found]
    (if-let [data (db/fetch db id)]
      (decode ((format-for this id) revision id) [(ByteBuffer/wrap data)])
      not-found))
  (assoc-node! [this id attrs]
    ;; TODO make assoc/get use new codecs opt-map api
    (db/put! db id (bufseq->bytes (encode ((format-for this id) revision id) attrs))))
  (dissoc-node! [this id]
    (db/delete! db id))

  Optimized
  (query-fn [layer keyseq f] nil)
  (update-fn [layer keyseq f]
    #_(when (= f adjoin) ;; TODO uncomment and fix this
        (fn [attrs]
          (db/append! db (node-meta-key id key)
                      (encode (node-meta-format revision id) attrs))
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
    (:revisions (raw-get-node this id nil)))

  ;; TODO these two are stubbed, will need to work eventually
  (get-changed-ids [layer rev]
    #{})
  (max-revision [layer] ;; how to do this? store in a meta-node? that adds a lot of writes
    nil)

  WrappedTransactional
  (txn-wrap [this f]
    ;; todo *skip-writes* for past revisions
    (fn [^MasaiLayer layer]
      (let [db-wrapped (txn-wrap db ; let db wrap transaction, but call f with layer
                                 (fn [_]
                                   (f layer)))]
        (db-wrapped (.db layer)))))

  Revisioned
  (at-revision [this rev]
    (assoc-record this :revision rev))
  (current-revision [this]
    revision)

  Preferences
  (manage-changelog? [this] false) ;; TODO wish this were more granular
  (manage-incoming? [this] true)
  (single-edge? [this] ;; TODO accept option to (make)
    false))

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

(defn- codec-fn [codec default]
  (as-fn (or codec default)))

;; formats should be functions from revision (and optionally node-id) to codec.
;; plain old codecs will be accepted as well
(defn make [db & [node-format meta-format layer-meta-format :as formats]]
  (let [[node-format meta-format layer-meta-format]
        (for [f (take 3 (concat formats (repeat nil)))]
          (codec-fn f (cereal/clojure-codec adjoin)))]
    (MasaiLayer. (make-db db) nil
                 node-format, meta-format, layer-meta-format)))
