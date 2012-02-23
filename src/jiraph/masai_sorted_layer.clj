(ns jiraph.masai-sorted-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer ChangeLog Meta Preferences
                             Schema node-id-seq meta-key meta-key?] :as layer]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        useful.debug
        [useful.utils :only [if-ns adjoin returning map-entry let-later empty-coll? copy-meta]]
        [useful.seq :only [find-with prefix-of?]]
        [useful.string :only [substring-after]]
        [useful.map :only [assoc-levels keyed]]
        [useful.fn :only [as-fn knit any to-fix]]
        [useful.io :only [long->bytes bytes->long]]
        [useful.datatypes :only [assoc-record]]
        [gloss.io :only [encode decode]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [masai.cursor :as cursor]
            [jiraph.graph :as graph]
            [jiraph.codecs.cereal :as cereal]
            [clojure.string :as s])
  (:import [java.nio ByteBuffer]))

(defn- no-nil-update
  "Update-in, but any value that would have become nil (or empty) is dissoc'ed entirely,
   recursively up to the root."
  ([m ks f & args]
     (no-nil-update m ks #(apply f % args)))
  ([m ks f]
     (if-let [[k & ks] (seq ks)]
       (let [v (no-nil-update (get m k) ks f)]
         (if (empty-coll? v)
           (dissoc m k)
           (assoc m k v)))
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

(defn along-path?
  "Is either of these a path-prefix of the other?"
  [pattern path]
  (every? true?
          (map (fn [x y]
                 (or (= x y) (= x :*) (= y :*)))
               pattern, path)))

(defn- subnode-codecs
  "This is used to find, given a path through a node and a sequence of [path,codec] pairs,
   all the codecs that will actually be needed to write at that path. Basically, this means all
   codecs whose path is below the write-path, and one codec which is at or above it."
  [codecs path]
  (let [path-to-root (filter #(along-path? (first %) path) codecs)
        [below [first-above]] (split-with #(path-prefix? path (first %) true)
                                          path-to-root)]
    (assert first-above (str "Don't know how to write at " path))
    `(~@below ~first-above)))

(defn matching-subpaths
  "Look through a node for all actual paths that match a path pattern. If include-empty? is truthy,
   return path even if there is no data at that path (but still iterate over * pattern entries)."
  [node path include-empty?]
  (if include-empty?
    (if (seq path)
      (let [path (vec path)
            [head last] ((juxt pop peek) path)]
        (if (= last :*)
          (for [entry (get-in node head)]
            (conj head (key entry)))
          [path]))
      '(()))
    ((fn matching* [node path]
       (if-let [[k & ks] (seq path)]
         (for [[k v] (if (= :* k)
                       node ;; each k/v in the node
                       (select-keys node [k])) ;; just the one
               path (matching* v ks)]
           (cons k path))
         '(())))
     node path)))

(defn- db-name
  "Convert a key sequence to a database keyname. Currently done by just joining them all together
   with : characters into a string."
  [keyseq]
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

(defn- codec-fns
  "Look up the codec functions to use based on node id and revision."
  [layer node-id revision]
  (let [locked {:id node-id, :revision revision}
        orig-paths (get layer (cond (= node-id (meta-key layer "_layer")) :layer-meta-format
                                    (meta-key? layer node-id) :node-meta-format
                                    :else :node-format))]
    (-> (for [[path codec-fn] (orig-paths locked)]
          [path (-> (fn [opts]
                      (codec-fn (merge opts locked)))
                    (copy-meta codec-fn))])
        (copy-meta orig-paths))))

(defn- realize-codecs
  "Realize a series of [path, codec-fn] pairs into [path, codec] by calling each codec-fn with
   the supplied argument opts."
  [path-codecs opts]
  (for [[path codec-fn] path-codecs]
    [path (codec-fn opts)]))

(defn- codecs-for
  "Shorthand for combining codec-fns and realize-codecs."
  [layer node-id revision]
  (realize-codecs (codec-fns layer node-id revision) {}))

(defn- delete-ranges!
  "Given a layer and a sequence of [start, end) intervals, delete every key in range. If the
   layer is in append-only mode, then a codec-fn must be included with each interval to enable
   us to encode a reset."
  [layer deletion-ranges]
  (doseq [{:keys [start stop codec-fn]} deletion-ranges]
    (let [delete (if (:append-only? layer)
                   (let-later [^:delay deleted (bufseq->bytes (encode (codec-fn {:reset true})
                                                                      {}))]
                     (fn [cursor]
                       (-> cursor
                           (cursor/append deleted)
                           (cursor/next))))
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

(let [revision-key "__revision"]
  (defn- save-maxrev [layer]
    (when-let [rev (:revision layer)]
      (db/put! (:db layer) revision-key (long->bytes rev))))
  (defn- read-maxrev [layer]
    (when-let [bytes (db/fetch (:db layer) revision-key)]
      (bytes->long bytes))))

(defn- node-chunks [codecs db id]
  (for [[path codec] codecs
        :let [{:keys [start stop parent keyfn]} (bounds (cons id path))
              kvs (seq (for [[k v] (db/fetch-seq db start)
                             :while (neg? (compare k stop))
                             :let [node (decode codec [(ByteBuffer/wrap v)])]
                             :when (not (empty-coll? node))]
                         (map-entry (keyfn k) node)))]
        :when kvs]
    (assoc-levels {} parent
                  (into {} kvs))))

(defn- read-node [codecs db id not-found]
  (reduce (fn ;; for an empty list (no keys found), reduce calls f with no args
            ([] not-found)
            ([a b] (adjoin a b)))
          (node-chunks codecs db id)))

(defn- optimized-writer
  "Return a writer iff the keyseq corresponds exactly to one path in path-codecs, and the
   corresponding codec has f as its reduce-fn (that is, we can apply the change to f by merely
   appending something to a single codec)."
  [layer path-codecs keyseq f]
  (when-not (next path-codecs) ;; can only optimize a single codec
    (let [[path codec-fn] (first path-codecs)]
      (when (= f (:reduce-fn (-> codec-fn meta))) ;; performing optimized function
        (let [[id & keys] keyseq]
          (when (= (count keys) (count path)) ;; at exactly this level
            (let [db (:db layer)
                  db-key (db-name keyseq) ;; great, we can optimize it
                  codec (codec-fn {})]
              (fn [arg] ;; TODO can we handle multiple args here? not sure how to encode that
                (db/append! db db-key (bufseq->bytes (encode codec arg)))
                {:old nil, :new nil} ;; we didn't read the old data, so we don't know the new data
                ))))))))

(defn- write-paths! [write-fn codecs id node include-deletions?]
  (reduce (fn [node [path codec]]
            (let [write! (fn [key data]
                           (->> data
                                (encode codec)
                                (bufseq->bytes)
                                (write-fn key)))]
              (reduce (fn [node path]
                        (let [path (vec path)
                              data (get-in node path)]
                          (write! (db-name (cons id path)) data)
                          (when (seq path)
                            (no-nil-update node (pop path) dissoc (peek path)))))
                      node (matching-subpaths node path include-deletions?))))
          (get node id), codecs))

(defn- simple-writer [layer path-codecs keyseq f]
  (let [{:keys [db append-only?]} layer
        [id & keys] keyseq
        codecs (realize-codecs path-codecs
                               (when append-only? {:reset true}))
        write-mode (if append-only?, db/append! db/put!)
        writer (partial write-mode db)
        deletion-ranges (for [[path codec-fn] path-codecs
                              :let [{:keys [start stop multi]} (bounds (cons id path))]
                              :when (and multi (prefix-of? (butlast path) keys))]
                          (keyed [start stop codec-fn]))]
    (fn [& args]
      (let [old (read-node codecs db id nil)
            new (apply update-in old keyseq f args)]
        (returning {:old (get-in old keyseq), :new (get-in new keyseq)}
          (delete-ranges! layer deletion-ranges)
          (write-paths! writer codecs id new true))))))

(defmulti specialized-writer
  "If your update function has special semantics that allow it to be distributed over multiple
   paths more efficiently than reading the whole node, applying the function, and then writing to
   each codec, you can implement a method for specialized-writer. For example, useful.utils/adjoin
   can be split up by matching up the paths in the adjoin-arg and in the path-codecs.

   path-codecs is a sequence of [path, codec-fn] pairs, which are computed for your convenience:
   if you preferred, you could recalculate them from layer and keyseq.
   You should return a function of one argument, which writes to the layer the result of
   (update-in layer keyseq f arg). See the contract for jiraph.layer/update-fn - you are
   effectively implementing an update-fn for a particular function rather than a layer.

   Note that it is acceptable to return nil instead of a function, if you find the keyseq or
   path-codecs mean you cannot do any optimization."
  (fn [layer path-codecs keyseq f]
    f))

(defmethod specialized-writer :default [& args]
  nil)

(defmethod specialized-writer adjoin [layer path-codecs keyseq _]
  (when (every? (fn [[path codec-fn]] ;; TODO support any reduce-fn
                  (= adjoin (:reduce-fn (meta codec-fn))))
                path-codecs)
    (let [db (:db layer)
          [id & keys] keyseq
          codecs (realize-codecs path-codecs {})
          writer (partial db/append! db)]
      (fn [arg]
        (returning {:old nil, :new arg}
          (write-paths! writer codecs id
                        (assoc-in {} keyseq arg)
                        false)))))) ;; don't include deletions


;;; TODO pull the three formats into a single field?
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
    ((layer/update-fn this [id] (constantly attrs)))
    true)
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
      (let [path-codecs (subnode-codecs (codec-fns this id revision) keys)]
        (assert (seq path-codecs) "No codecs to write with")
        (some #(% this path-codecs keyseq f)
              [specialized-writer, optimized-writer, simple-writer]))))

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

  Schema
  (schema [this id]
    (:schema (meta (codec-fns this id revision))))
  (verify-node [this id attrs]
    (try
      ;; do a fake write (does no I/O), to see if an exception would occur
      (do (write-paths! (constantly nil), (codecs-for this id revision),
                        id, attrs, false)
          true)
      (catch Exception _ false)))

  ChangeLog
  (get-revisions [this id]
    (let [path-codecs (codec-fns this id revision)
          revision-codecs (for [[path codec-fn] path-codecs
                                :let [codec (-> codec-fn meta :revisions)]
                                :when codec]
                            [path (codec {})])
          revs (->> (node-chunks revision-codecs db id)
                    (tree-seq (any map? sequential?) (to-fix map? vals,
                                                             sequential? seq))
                    (filter number?)
                    (into (sorted-set)))]
      (if revision
        (subseq revs <= revision)
        revs)))

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
      codec-fn      (fn [codec]
                      (let [codec (or codec default-codec)]
                        (-> (as-fn codec) (copy-meta codec))))]
  (defn make [db & {{:keys [node meta layer-meta]
                     :or {node (-> [[[:edges :*]]
                                    [[]]]
                                   (with-meta layer/edges-schema))}} :formats,

                     :keys [assoc-mode] :or {assoc-mode :append}}]
    (let [[node-format meta-format layer-meta-format]
          (for [format [node meta layer-meta]]
            (if (seq format)
              (-> (for [[path f] format]
                    (map-entry path (codec-fn f)))
                  (copy-meta format))
              (-> [[[] (codec-fn nil)]]
                  (vary-meta merge {:schema {:type :map, :fields {:* :any}}}))))]
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
