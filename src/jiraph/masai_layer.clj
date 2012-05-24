(ns jiraph.masai-layer
  (:use [jiraph.layer :as layer
         :only [Enumerate Optimized Basic Layer ChangeLog Preferences Schema node-id-seq]]
        [jiraph.formats :only [special-codec]]
        [jiraph.utils :only [meta-id meta-id? base-id id->str]]
        [jiraph.codex :only [encode decode]]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        [useful.utils :only [if-ns adjoin returning]]
        [useful.seq :only [find-with]]
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

(defn- bytes->long [bytes]
  (-> bytes (ByteArrayInputStream.) (DataInputStream.) (.readLong)))

(defn- long->bytes [long]
  (-> (ByteArrayOutputStream. 8)
      (doto (-> (DataOutputStream.) (.writeLong long)))
      (.toByteArray)))

(let [revision-key "__revision"]
  (defn- save-maxrev [layer]
    (when-let [rev (:revision layer)]
      (db/put! (:db layer) revision-key (long->bytes rev))
      (swap! (:max-written-revision layer)
             (fn [cached-max]
               (max rev (or cached-max 0))))))
  (defn- read-maxrev [layer]
    (swap! (:max-written-revision layer)
           (fn [cached-max]
             (or cached-max
                 (if-let [bytes (db/fetch (:db layer) revision-key)]
                   (bytes->long bytes)
                   0))))))

(defn revision-to-read [layer]
  (let [revision (:revision layer)]
    (and revision
         (let [max-written (read-maxrev layer)]
           (when (< revision max-written)
             revision)))))

(defrecord MasaiLayer [db revision max-written-revision append-only?
                       node-format-fn node-meta-format-fn layer-meta-format-fn]
  Object
  (toString [this]
    (pr-str this))

  Enumerate
  (node-id-seq [this]
    (remove meta-id? (db/key-seq db)))
  (node-seq [this]
    (map #(graph/get-node this %) (node-id-seq this)))

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
    (when-let [[id & keys] (seq keyseq)]
      (let [{:keys [reduce-fn codec]} (format-for this id revision)]
        (when (= f reduce-fn)
          (fn [attrs]
            (->> (if keys
                   (assoc-in {} keys attrs)
                   attrs)
                 (encode codec)
                 (db/append! db (id->str id)))
            {:old nil :new attrs})))))

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
    (swap! (:max-written-revision this)
           (constantly nil)))

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
    (when-let [rev-codec (:revisions (format-for this id nil))]
      (when-let [data (db/fetch db (id->str id))]
        (let [revs (decode rev-codec data)]
          (distinct
           (if-not revision
             revs
             (take-while #(<= % revision) revs)))))))

  ;; TODO this is stubbed, will need to work eventually
  (get-changed-ids [this rev]
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
      (MasaiLayer. (make-db db) nil (atom nil)
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
