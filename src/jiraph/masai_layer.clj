(ns jiraph.masai-layer
  (:use [jiraph.layer :as layer
         :only [Enumerate Optimized Historical Basic Layer ChangeLog Preferences Schema
                node-id-seq]]
        [jiraph.formats :only [special-codec]]
        [jiraph.utils :only [meta-id meta-id? base-id id->str meta-str?]]
        [jiraph.codex :only [encode decode]]
        [jiraph.masai-common :only [implement-ordered revision-to-read]]
        [retro.core   :only [Transactional Revisioned OrderedRevisions
                             txn-begin! txn-commit! txn-rollback!]]
        [useful.utils :only [if-ns adjoin returning map-entry]]
        [useful.seq :only [find-with]]
        [useful.state :only [volatile put!]]
        [useful.fn :only [as-fn fix given]]
        [useful.datatypes :only [assoc-record]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [jiraph.graph :as graph]
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

(defn- format-for [layer node-id revision]
  (let [format-fn (get layer (cond (= node-id (meta-id :layer)) :layer-meta-format-fn
                                   (meta-id? node-id)           :node-meta-format-fn
                                   :else                        :node-format-fn))]
    (format-fn {:id node-id :revision revision})))

(defn- revision-seq [format revision bytes]
  (when-let [rev-codec (:revisions format)]
    (let [revs (decode rev-codec bytes)]
      (distinct
       (if-not revision
         revs
         (take-while #(<= % revision) revs)))))  )

(defrecord MasaiLayer [db revision max-written-revision append-only?
                       node-format-fn node-meta-format-fn layer-meta-format-fn]
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
      (decode (:codec (format-for this id (revision-to-read this)))
              data)
      not-found))
  (assoc-node! [this id attrs]
    (letfn [(bytes [data]
              (let [format (format-for this id revision)
                    codec (or (and append-only?
                                   (:reset format))
                              (:codec format))]
                (encode codec data)))]
      ((if append-only? db/append! db/put!)
       db (id->str id) (bytes attrs))))
  (dissoc-node! [this id]
    (db/delete! db (id->str id)))

  Optimized
  (query-fn [this keyseq not-found f] nil)
  (update-fn [this keyseq f]
    (when append-only?
      (when-let [[id & keys] (seq keyseq)]
        (let [{:keys [reduce-fn codec]} (format-for this id revision)]
          (when (= f reduce-fn)
            (fn [attrs]
              (->> (if keys
                     (assoc-in {} keys attrs)
                     attrs)
                   (encode codec)
                   (db/append! db (id->str id)))
              {:old nil :new attrs}))))))

  Layer
  (open [this]
    (db/open db))
  (close [this]
    (db/close db))
  (sync! [this]
    (db/sync! db))
  (optimize! [this]
    (db/optimize! db))
  (truncate! [this]
    (db/truncate! db)
    (put! max-written-revision nil))

  Schema
  (schema [this id]
    (:schema (format-for this id revision)))
  (verify-node [this id attrs]
    (try
      ;; do a fake write (does no I/O), to see if an exception would occur
      (encode (:codec (format-for this id revision))
              attrs)
      (catch Exception _ false)))

  ChangeLog
  (get-revisions [this id]
    (when-let [data (db/fetch db (id->str id))]
      (revision-seq (format-for this id nil) revision data)))

  Historical
  (node-history [this id]
    (when-let [data (db/fetch db (id->str id))]
      (if-let [historical-codec (:historical (format-for this id revision))]
        (decode historical-codec data)
        (when-let [revisions (revision-seq (format-for this id nil) revision data)]
          (into (sorted-map)
                (for [revision revisions]
                  [revision (decode (:codec (format-for this id revision))
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
    revision)

  Preferences
  (manage-changelog? [this] false) ;; TODO wish this were more granular
  (manage-incoming? [this] true)
  (single-edge? [this] ;; TODO accept option to (make)
    false))

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
  (defn make [db & {{:keys [node meta layer-meta]} :format-fns,
                    :keys [assoc-mode] :or {assoc-mode :append}}]
    (let [[node-format-fn meta-format-fn layer-meta-format-fn]
          (map #(as-fn (or % default-format-fn))
               [node meta layer-meta])]
      (MasaiLayer. (make-db db) nil (volatile nil)
                   (case assoc-mode
                     :append true
                     :overwrite false)
                   node-format-fn, meta-format-fn, layer-meta-format-fn))))

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
