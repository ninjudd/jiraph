(ns jiraph.masai-sorted-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer ChangeLog Meta Preferences
                             node-id-seq meta-key meta-key?] :as layer]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        useful.debug
        [useful.utils :only [if-ns adjoin returning map-entry]]
        [useful.seq :only [find-with]]
        [useful.string :only [substring-after]]
        [useful.map :only [assoc-levels]]
        [useful.fn :only [as-fn knit]]
        [useful.datatypes :only [assoc-record]]
        [gloss.io :only [encode decode]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [jiraph.graph :as graph]
            [jiraph.codecs.cereal :as cereal]
            [clojure.string :as s])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader
            DataOutputStream DataInputStream]
           [java.nio ByteBuffer]))

(defn- path-prefix?
  "This is a lot like prefix-of? in useful, but treats :* as equal to everything and has
  a \"strict\" mode for requiring strict (not equal-length) prefixes."
  ([pattern path]
     (path-prefix? pattern path false))
  ([pattern path strict?]
     (loop [pattern pattern, path path]
       (if (empty? pattern)
         (or (not strict?)
             (seq path))
         (and (seq path)
              (let [[x & xs] pattern
                    [y & ys] path]
                (and (or (= x y) (= x :*) (= y :*))
                     (recur xs ys))))))))

(defn- strict-prefix?
  [pattern path])

(defn- subnode-codecs [codecs path]
  (let [path-to-root (filter #(path-prefix? (first %) path) codecs)
        [below [first-above]] (split-with #(path-prefix? path (first %) true)
                                          path-to-root)]
    (assert first-above (str "Don't know how to write at " path))
    `(~@below ~first-above)))

(defn- fill-pattern [pattern actual]
  (map (fn [pat act] act) pattern actual))

(defn- ignore? [x]
  (or (nil? x)
      (and (coll? x)
           (empty? x))))

(let [char-after (fn [c]
                   (char (inc (int c))))
      after-colon (char-after \:)
      str-after (fn [s] ;; the string immediately after this one in lexical order
                  (str s \u0001))]
  (defn bounds [path]
    (let [path       (vec path)
          last       (peek path)
          multi?     (= :* last)
          path       (pop path)
          top-level? (empty? path)
          start      (s/join ":" (map name path))]
      (if top-level?
        {:start last, :stop (str-after last)
         :keyfn (constantly last), :parent []}
        (into {:parent path}
              (if multi?
                {:start (str start ":")
                 :stop (str start after-colon)
                 :keyfn (substring-after ":")}
                (let [start-key (str start ":" (name last))]
                  {:start start-key
                   :stop (str-after start-key)
                   :keyfn (constantly last)})))))))

(defn- codecs-for [layer node-id revision]
  (for [[path codec-fn] (get layer (cond (= node-id (meta-key layer "_layer")) :layer-meta-format
                                         (meta-key? layer node-id) :node-meta-format
                                         :else :node-format))]
    [path (codec-fn {:revision revision, :id node-id})]))

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

(defn- read-node [codecs id not-found]
  (reduce (fn ;; for an empty list (no keys found), reduce calls with no args
            ([] not-found)
            ([a b] (adjoin a b)))
          (for [[path codec] codecs
                :let [{:keys [start stop parent keyfn]} (bounds (cons id path))
                      kvs (seq (for [[k v] (db/fetch-seq db start)
                                     :while (neg? (compare k stop))
                                     :let [node (decode codec [(ByteBuffer/wrap v)])]
                                     :when (not (ignore? node))]
                                 (map-entry (keyfn k) node)))]
                :when kvs]
            (assoc-levels {} parent
                          (into {} kvs)))))

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
    (let [node (read-node (codecs-for this id revision) db id not-found)]
      (if (identical? node not-found)
        not-found
        (get node id))))
  (assoc-node! [this id attrs]
    #_(letfn [(bytes [data]
              (bufseq->bytes (encode ((format-for this id) {:revision revision :id id})
                                     data)))]
      (if append-only?
        (db/append! db id (bytes (assoc attrs :_reset true)))
        (db/put!    db id (bytes attrs)))))
  (dissoc-node! [this id]
    (db/delete! db id))

  Optimized
  (query-fn [this keyseq f]
    (let [[id & keys] keyseq]
      (if-let [codecs (->> (codecs-for this id revision)
                           (filter #(path-prefix? (key %) keys))
                           (seq))] ;; going to be testing it for truthiness
        (fn [& args]
          (apply f (get-in (read-node codecs db id nil)
                           keyseq)
                 args))
        (throw (Exception. "Trying to read at a level where we have no codecs...?")))))
  (update-fn [this keyseq f]
    #_
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
    #_
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

(if-ns (:require [masai.tokyo-sorted :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

;; formats should be functions:
;; - accept as arg: a map containing {revision and node-id}
;; - return: a gloss codec
;; plain old codecs will be accepted as well
(let [default-codec (cereal/revisioned-clojure-codec adjoin)
      codec-fn      (fn [codec] (as-fn (or codec default-codec)))]
  (defn make [db & {{:keys [node meta layer-meta]} :formats,
                    :keys [assoc-mode] :or {assoc-mode :append}}]
    (let [[node-format meta-format layer-meta-format]
          (for [format [node meta layer-meta]]
            (for [[path codec] format]
              (map-entry path (codec-fn codec))))]
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
