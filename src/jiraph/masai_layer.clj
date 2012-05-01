(ns jiraph.masai-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer ChangeLog Meta Preferences Schema
                             node-id-seq meta-key meta-key?] :as layer]
        [jiraph.codecs :only [special-codec encode decode]]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        [useful.utils :only [if-ns adjoin returning]]
        [useful.seq :only [find-with]]
        [useful.fn :only [as-fn fix given]]
        [useful.datatypes :only [assoc-record]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [jiraph.graph :as graph]
            [jiraph.codecs.cereal :as cereal])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader
            DataOutputStream DataInputStream]
           [java.nio ByteBuffer]))

(defn- codec-for [layer node-id revision]
  (let [codec-fn (get layer (cond (= node-id (meta-key layer "_layer")) :layer-meta-codec-fn
                                  (meta-key? layer node-id) :node-meta-codec-fn
                                  :else :node-codec-fn))]
    (codec-fn {:id node-id :revision revision})))

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
                       node-codec-fn node-meta-codec-fn layer-meta-codec-fn]
  Object
  (toString [this]
    (pr-str this))

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
      (decode (codec-for this id (revision-to-read this))
              [(ByteBuffer/wrap data)])
      not-found))
  (assoc-node! [this id attrs]
    (letfn [(bytes [data]
              (let [codec (-> (codec-for this id revision)
                              (given append-only? (special-codec :reset)))]
                (bufseq->bytes (encode codec data))))]
      ((if append-only? db/append! db/put!)
       db id (bytes attrs))))
  (dissoc-node! [this id]
    (db/delete! db id))

  Optimized
  (query-fn [this keyseq not-found f] nil)
  (update-fn [this keyseq f]
    (when-let [[id & keys] (seq keyseq)]
      (let [codec (codec-for this id revision)]
        (when (= f (:reduce-fn (meta codec)))
          (fn [attrs]
            (->> (if keys
                   (assoc-in {} keys attrs)
                   attrs)
                 (encode codec)
                 (bufseq->bytes)
                 (db/append! db id))
            {:old nil :new attrs})))))

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
    (db/truncate! db)
    (swap! (:max-written-revision layer)
           (constantly nil)))

  Schema
  (schema [this id]
    (:schema (meta (codec-for this id revision))))
  (verify-node [this id attrs]
    (try
      ;; do a fake write (does no I/O), to see if an exception would occur
      (dorun (bufseq->bytes (encode (codec-for this id revision) attrs)))
      (catch Exception _ false)))

  ChangeLog
  (get-revisions [this id]
    (when-let [rev-codec (:revisions (meta (codec-for this id nil)))]
      (when-let [data (db/fetch db id)]
        (let [revs (decode rev-codec [(ByteBuffer/wrap data)])]
          (distinct
           (if-not revision
             revs
             (take-while #(<= % revision) revs)))))))

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

(if-ns (:require [masai.tokyo :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

;; codec-fns should be functions:
;; - accept as arg: a map containing {revision and node-id}
;; - return: a gloss codec
;; plain old codecs will be accepted as well
(let [default-codec (cereal/revisioned-clojure-codec adjoin)
      codec-fn      (fn [codec]
                      (as-fn (or codec default-codec)))]
  (defn make [db & {{:keys [node meta layer-meta]
                     :or {node (-> (codec-fn default-codec)
                                   (vary-meta merge layer/edges-schema))}} :codec-fns,
                    :keys [assoc-mode] :or {assoc-mode :append}}]
    (let [[node-codec-fn meta-codec-fn layer-meta-codec-fn]
          (map codec-fn [node meta layer-meta])]
      (MasaiLayer. (make-db db) nil (atom nil)
                   (case assoc-mode
                     :append true
                     :overwrite false)
                   node-codec-fn, meta-codec-fn, layer-meta-codec-fn))))

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
