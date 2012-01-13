(ns jiraph.masai-sorted-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer ChangeLog Meta Preferences
                             node-id-seq meta-key meta-key?] :as layer]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        useful.debug
        [useful.utils :only [if-ns adjoin returning map-entry let-later]]
        [useful.seq :only [find-with prefix-of?]]
        [useful.string :only [substring-after]]
        [useful.map :only [assoc-levels]]
        [useful.fn :only [as-fn knit]]
        [useful.datatypes :only [assoc-record]]
        [gloss.io :only [encode decode]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [masai.cursor :as cursor]
            [jiraph.graph :as graph]
            [jiraph.codecs.cereal :as cereal]
            [clojure.string :as s])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader
            DataOutputStream DataInputStream]
           [java.nio ByteBuffer]))

(defn- no-nil-update
  ([m ks f & args]
     (no-nil-update m ks #(apply f % args)))
  ([m ks f]
     (if-let [[k & ks] (seq ks)]
       (let [v (no-nil-update (get m k) ks f)]
         (if (and (not (nil? v))
                  (or (not (coll? v))
                      (seq v)))
           (assoc m k v)
           (dissoc m k)))
       (f m))))

(defn- path-prefix?
  "This is a lot like prefix-of? in useful, but treats :* as equal to everything and has
  a \"strict\" mode for requiring strict (not equal-length) prefixes."
  ([pattern path]
     (path-prefix? pattern path false))
  ([pattern path strict?]
     (loop [pattern pattern, path path]
       (if (empty? pattern)
         (or (not strict?)
             (and (seq path)
                  (not= [:*] path)))
         (and (seq path)
              (let [[x & xs] pattern
                    [y & ys] path]
                (and (or (= x y) (= x :*) (= y :*))
                     (recur xs ys))))))))

(defn along-path? [pattern path]
  (every? true?
          (map (fn [x y]
                 (or (= x y) (= x :*) (= y :*)))
               pattern, path)))

(defn- subnode-codecs [codecs path]
  (let [path-to-root (filter #(along-path? (first %) path) codecs)
        [below [first-above]] (split-with #(path-prefix? path (first %) true)
                                          path-to-root)]
    (assert first-above (str "Don't know how to write at " path))
    `(~@below ~first-above)))

(defn matching-subpaths [node path]
  (if-let [[k & ks] (seq path)]
    (for [[k v] (if (= :* k)
                  node ;; each k/v in the node
                  (select-keys node [k])) ;; just the one
          path (matching-subpaths v ks)]
      (cons k path))
    '(())))

(defn- fill-pattern [pattern actual]
  (map (fn [pat act] act) pattern actual))

(defn- ignore? [x]
  (or (nil? x)
      (and (coll? x)
           (empty? x))))

(defn- db-name [keyseq]
  (s/join ":" (map name keyseq)))

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
          start      (db-name path)]
      (if top-level?
        {:start last, :stop (str-after last)
         :keyfn (constantly last), :parent []}
        (into {:parent path}
              (if multi?
                {:start (str start ":")
                 :stop (str start after-colon)
                 :keyfn (substring-after ":")
                 :multi true}
                (let [start-key (str start ":" (name last))]
                  {:start start-key
                   :stop (str-after start-key)
                   :keyfn (constantly last)})))))))

(defn- codecs-for [layer node-id revision]
  (for [[path codec-fn] (get layer (cond (= node-id (meta-key layer "_layer")) :layer-meta-format
                                         (meta-key? layer node-id) :node-meta-format
                                         :else :node-format))]
    [path (codec-fn {:revision revision, :id node-id})]))

(defn- delete-ranges!
  "Given a database and a sequence of [start, end) intervals, delete "
  [layer deletion-ranges]
  (doseq [{:keys [start stop codec]} deletion-ranges]
    (let [delete (if (:append-only? layer)
                   (let-later [^:delay deleted (bufseq->bytes (encode codec {:_reset true}))]
                     (fn [cursor]
                       (cursor/append cursor deleted)))
                   cursor/delete)]
      (loop [cur (db/cursor (:db layer) start)]
        (when-let [k (cursor/key cur)]
          (when-let [cur (and (neg? (compare (String. k) stop))
                              (delete cur))]
            (recur cur)))))))

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

(defn- read-node [codecs db id not-found]
  (reduce (fn ;; for an empty list (no keys found), reduce calls f with no args
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

(defrecord MasaiSortedLayer [db revision append-only? node-format node-meta-format layer-meta-format]
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
    (let [[id & keys] keyseq
          codecs (subnode-codecs (codecs-for this id revision) keys)]
      (assert (seq codecs) "Trying to read at a level where we have no codecs...?")
      (fn [& args]
        (apply f (get-in (read-node codecs db id nil)
                         keyseq)
               args))))
  (update-fn [this keyseq f]
    (when-let [[id & keys] (seq keyseq)]
      (let [codecs (subnode-codecs (codecs-for this id revision) keys)
            deletion-ranges (for [[path codec] codecs
                                  :let [{:keys [start stop multi]} (bounds path)]
                                  :when (and multi (prefix-of? (butlast path) keys))]
                              {:start (str id ":" start)
                               :stop  (str id ":" stop)
                               :codec codec})]
        (assert (seq codecs) "No codecs to write with")
        ;; ...TOOD special-case adjoin...
        (or (and (not (next codecs)) ;; only one codec, see if we can optimize writing it
                 (let [[path codec] (first codecs)]
                   (and (= f (:reduce-fn (-> codec meta))) ;; performing optimized function
                        (= (count keys) (count path)) ;; at exactly this level
                        (let [db-key (db-name keyseq)] ;; great, we can optimize it
                          (fn [arg] ;; TODO can we handle multiple args here? not sure how to encode that
                            (db/append! db db-key (bufseq->bytes (encode codec arg))))))))

            (fn [& args]
              (let [old (-> (read-node codecs db id nil)
                            (get id))
                    new (apply update-in old keys f args)]
                (delete-ranges! this deletion-ranges)
                (loop [codecs codecs, node new]
                  (if-let [[[path codec] & more] (seq codecs)]
                    (recur more (reduce (fn [node path]
                                          (let [path (vec path)
                                                data (get-in node path)
                                                old-data (get-in old path)]
                                            (db/put! db (db-name (cons id path))
                                                     (bufseq->bytes (encode codec data)))
                                            (no-nil-update node (pop path) dissoc (peek path))))
                                        node, (matching-subpaths node path)))
                    {:old (get-in old keys), :new (get-in new keys)}))))))))

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
    (fn [^MasaiSortedLayer layer]
      (let [db-wrapped (txn-wrap db     ; let db wrap transaction, but call f with layer
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
      (MasaiSortedLayer. (make-db db) nil
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
