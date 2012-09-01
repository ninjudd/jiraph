(ns jiraph.masai-layer
  (:use [jiraph.layer :as layer
         :only [Enumerate Optimized Historical Basic Layer ChangeLog Schema
                node-id-seq]]
        [jiraph.formats :only [special-codec]]
        [jiraph.utils :only [meta-id meta-id? base-id id->str meta-str?]]
        [jiraph.codex :only [encode decode]]
        [jiraph.masai-common :only [implement-ordered revision-to-read]]
        [retro.core :only [Transactional Revisioned OrderedRevisions
                           at-revision txn-begin! txn-commit! txn-rollback!]]
        [useful.utils :only [if-ns adjoin returning map-entry]]
        [useful.seq :only [find-with]]
        [useful.state :only [volatile put!]]
        [useful.fn :only [as-fn fix given]]
        [useful.datatypes :only [assoc-record]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [jiraph.graph :as graph :refer [with-action]]
            [jiraph.formats.cereal :as cereal])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader
            DataOutputStream DataInputStream]
           [java.nio ByteBuffer]))

;;; A masai-layer "format" is a map containing, at least, a Gloss codec for encoding nodes,
;;; under the :codec key. Optional components:
;;; - a Schematic :schema
;;; - the :reduce-fn the codec uses when combining revisions
;;;   - this is used to optimize updates
;;; - a :reset codec, for writing data that should not use the reduce-fn
;;;   - this codec need not be capable of reading - it will only be written with
;;; - a :revisions codec, for reading the list of revisions at which a node has been touched.

(defn write-format [layer node-id]
  ((:format-fn layer) node-id (:revision layer)))

(defn read-format [layer node-id]
  ((:format-fn layer) node-id (revision-to-read layer)))

(defn- revision-seq [format revision bytes]
  (when-let [rev-codec (:revisions format)]
    (let [revs (decode rev-codec bytes)]
      (distinct
       (if-not revision
         revs
         (take-while #(<= % revision) revs)))))  )

(defrecord MasaiLayer [db revision max-written-revision append-only? format-fn]
  Object
  (toString [this]
    (pr-str this))

  Enumerate
  (node-id-seq [this]
    (remove meta-str? (db/key-seq db)))
  (node-seq [this]
    (for [id (node-id-seq this)]
      (map-entry id (graph/get-node this id))))

  Basic
  (get-node [this id not-found]
    (if-let [data (db/fetch db (id->str id))]
      (decode (:codec (read-format this id))
              data)
      not-found))
  (assoc-node [this id attrs]
    (letfn [(bytes [layer data]
              (let [format (write-format layer id)
                    codec (or (and append-only?
                                   (:reset format))
                              (:codec format))]
                (encode codec data)))]
      (with-action [layer this] {:old nil, :new attrs}
        ((if append-only? db/append! db/put!)
         db (id->str id) (bytes layer attrs)))))
  (dissoc-node [this id]
    (with-action [layer this] nil
      (db/delete! db (id->str id))))

  Optimized
  (query-fn [this keyseq not-found f] nil)
  (update-fn [this keyseq f]
    (when-let [[id & keys] (seq keyseq)]
      (when (= f (:reduce-fn (write-format this id)))
        (fn [attrs]
          (with-action [layer this] {:old nil :new attrs}
            (->> (if keys
                   (assoc-in {} keys attrs)
                   attrs)
                 (encode (:codec (write-format layer id)))
                 (db/append! db (id->str id))))))))

  Layer
  (open [this]
    (db/open db))
  (close [this]
    (db/close db))
  (sync [this]
    (with-action [layer this] nil
      (db/sync! db)))
  (optimize [this]
    (with-action [layer this] nil
      (db/optimize! db)))
  (truncate [this]
    (with-action [layer this] nil
      (db/truncate! db)
      (put! max-written-revision nil)))

  Schema
  (schema [this id]
    (:schema (read-format this id)))
  (verify-node [this id attrs]
    (try
      ;; do a fake write (does no I/O), to see if an exception would occur
      (encode (:codec (write-format this id))
              attrs)
      (catch Exception _ false)))

  ChangeLog
  (get-revisions [this id]
    (when-let [data (db/fetch db (id->str id))]
      (revision-seq (read-format (at-revision this nil) id) revision data)))

  Historical
  (node-history [this id]
    (when-let [data (db/fetch db (id->str id))]
      (if-let [historical-codec (:historical (read-format this id))]
        (decode historical-codec data)
        (when-let [revisions (revision-seq (read-format (at-revision this nil)
                                                        id)
                                           revision data)]
          (into (sorted-map)
                (for [revision revisions]
                  [revision (decode (:codec
                                     (read-format (at-revision this revision)
                                                  id))
                                    data)]))))))

  ;; TODO this is stubbed, will need to work eventually
  (get-changed-ids [this rev]
    #{})

  Transactional
  (txn-begin! [this]
    (txn-begin! db))
  (txn-commit! [this]
    (txn-commit! db))
  (txn-rollback! [this]
    (put! max-written-revision nil)
    (txn-rollback! db))

  Revisioned
  (at-revision [this rev]
    (assoc-record this :revision rev))
  (current-revision [this]
    revision))

(implement-ordered MasaiLayer)

(if-ns (:require [masai.tokyo :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

(let [default-format-fn (cereal/revisioned-clojure-format adjoin)]
  ;; format-fn should be a function:
  ;; - accept as arg: a map containing {revision and node-id}
  ;; - return: a format (see doc for formats at the top of this file)
  (defn make [db & {:keys [assoc-mode format-fn] :or {assoc-mode :append}}]
    (MasaiLayer. (make-db db) nil (volatile nil)
                 (case assoc-mode
                   :append true
                   :overwrite false)
                 (as-fn (or format-fn default-format-fn)))))

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

;; TODO wth is this? Must be some scratch work I committed accidentally?
(def codex (let [type->num {"profile" 1
                            "union" 2}
                 num->type (into {} (for [[k v] type->num] [v k]))
                 write-id (fn [^DataOutputStream out ^String node-id inc?]
                            (let [[node-type id-str] (clojure.string/split node-id #"-")
                                  type-abbrev (get type->num node-type)
                                  id-num (Long/parseLong id-str)]
                              (.writeByte out type-abbrev)
                              (.writeLong out (fix id-num inc? inc))))]
             {:read (fn [bytes]
                      )
              :write (let [key-len 9] ;; one-byte type, 8-byte long for id
                       (fn [keyseq]
                         (let [[head & tail] keyseq
                               edge? (= :edges (first tail))]
                           (if (and edge? (not (next tail)))
                             nil ;; Don't know how to handle [id :edge] with no futher tail
                             (let [edge-to (and edge? (second tail))
                                   range? (keyword? edge-to)
                                   len (cond range? (inc key-len)
                                             edge? (* 2 key-len)
                                             :else key-len)
                                   buf (ByteArrayOutputStream. len)
                                   writer (DataOutputStream. buf)]
                               (write-id writer head (and range? (= :max edge-to)))
                               (when edge?
                                 (case edge-to
                                   :min (.writeByte writer (byte 0))
                                   :max nil
                                   (write-id writer edge-to)))
                               (.toByteArray buf))))))}))
