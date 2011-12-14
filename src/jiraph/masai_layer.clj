(ns jiraph.masai-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer ChangeLog Meta Preferences
                             node-id-seq meta-key meta-key?] :as layer]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        [useful.utils :only [if-ns adjoin returning]]
        [useful.seq :only [find-with]]
        [useful.fn :only [as-fn]]
        [useful.datatypes :only [assoc-record]]
        [gloss.io :only [encode decode]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [jiraph.graph :as graph]
            [jiraph.codecs.cereal :as cereal])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader
            DataOutputStream DataInputStream]
           [java.nio ByteBuffer]))

(defn- format-for [layer node-id]
  (get layer (cond (= node-id (meta-key layer "_layer")) :layer-meta-format
                   (meta-key? layer node-id) :node-meta-format
                   :else :node-format)))

;; drop leading _ - NB must undo the meta-key impl in MasaiLayer
(defn- main-node-id [meta-id]
  {:pre [(= "_" (first meta-id))]}
  (subs meta-id 1))

(defn- bytes->long [bytes]
  (-> bytes (ByteArrayInputStream.) (DataInputStream.) (.readLong)))

(defn- long->bytes [long]
  (-> (ByteArrayOutputStream. 8)
      (doto (-> (DataOutputStream.) (.writeLong long)))
      (.toByteArray)))

(let [revision-key "__revision"]
  (defn- save-maxrev [layer]
    (when-let [rev (:revision layer)]
      (db/put! (:db layer) revision-key (long->bytes rev))))
  (defn- read-maxrev [layer]
    (when-let [bytes (db/fetch (:db layer) revision-key)]
      (bytes->long bytes))))

(defrecord MasaiLayer [db revision append-only? node-format node-meta-format layer-meta-format]
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
      (decode ((format-for this id) {:revision revision :id id})
              [(ByteBuffer/wrap data)])
      not-found))
  (assoc-node! [this id attrs]
    (letfn [(bytes [data]
              (bufseq->bytes (encode ((format-for this id) {:revision revision :id id})
                                     data)))]
      (if append-only?
        (db/append! db id (bytes (assoc attrs :_reset true)))
        (db/put!    db id (bytes attrs)))))
  (dissoc-node! [this id]
    (db/delete! db id))

  Optimized
  (query-fn [this keyseq f] nil)
  (update-fn [this keyseq f]
    (when-let [[id & keys] (seq keyseq)]
      (let [encoder (format-for this id)]
        (when (= f (:reduce-fn (meta encoder)))
          (fn [m]
            (db/append! db id
                        (bufseq->bytes (encode (encoder {:revision revision})
                                               (if keys
                                                 (assoc-in {} keys m)
                                                 m))))
            {:old nil :new m})))))

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
    (let [format (format-for this id)
          rev-codec-builder (-> format meta :revisions)
          rev-codec (rev-codec-builder {})]
      (when-let [data (db/fetch db id)]
        (let [revs (decode rev-codec [(ByteBuffer/wrap data)])]
          (if-not revision
            revs
            (take-while #(<= % revision) revs))))))

  ;; TODO this is stubbed, will need to work eventually
  (get-changed-ids [layer rev]
    #{})

  ;; TODO: Problems with implicit transaction-wrapping: we end up writing that the revision has
  ;; been applied, and refusing to do any more writing to that revision. What is the answer?
  WrappedTransactional
  (txn-wrap [this f]
    ;; todo *skip-writes* for past revisions
    (fn [^MasaiLayer layer]
      (let [db-wrapped (txn-wrap db ; let db wrap transaction, but call f with layer
                                 (fn [_]
                                   (returning (f layer)
                                     (save-maxrev layer))))]
        (db-wrapped (.db layer)))))

  Revisioned
  (at-revision [this rev]
    (assoc-record this :revision rev))
  (current-revision [this]
    revision)

  OrderedRevisions
  (max-revision [this]
    (read-maxrev this))

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

;; formats should be functions from revision (and optionally node-id) to codec.
;; plain old codecs will be accepted as well
(let [default-codec (cereal/revisioned-clojure-codec adjoin)
      codec-fn      (fn [codec] (as-fn (or codec default-codec)))]
  (defn make [db & {{:keys [node meta layer-meta]} :formats,
                    :keys [assoc-mode] :or {assoc-mode :append}}]
    (let [[node-format meta-format layer-meta-format]
          (map codec-fn [node meta layer-meta])]
      (MasaiLayer. (make-db db) nil
                   (case assoc-mode
                     :append true
                     :overwrite false)
                   node-format, meta-format, layer-meta-format))))

(defn temp-layer
  "Create a masai layer on a temporary file, deleting the file when the JVM exits.
   Returns a pair of [file layer]."
  [& args]
  (let [file (java.io.File/createTempFile "layer" "db")
        name (.getAbsolutePath file)]
    (returning [file (apply make name args)]
      (.deleteOnExit file))))

(def make-temp (comp second temp-layer))

(defmacro with-temp-layer [[binding & args] & body]
  `(let [[file# layer#] (temp-layer ~@args)
         ~binding layer#]
     (layer/open layer#)
     (returning ~@body
       (layer/close layer#)
       (.delete file#))))
