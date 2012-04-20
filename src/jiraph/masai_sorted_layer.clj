(ns jiraph.masai-sorted-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer ChangeLog Meta Preferences
                             Schema node-id-seq meta-key meta-key?] :as layer]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        useful.debug
        [useful.utils :only [invoke if-ns adjoin returning map-entry empty-coll? copy-meta]]
        [useful.seq :only [find-with prefix-of?]]
        [useful.string :only [substring-after]]
        [useful.map :only [assoc-levels map-vals keyed]]
        [useful.fn :only [as-fn knit any fix to-fix ! validator]]
        [useful.io :only [long->bytes bytes->long]]
        [useful.datatypes :only [assoc-record]]
        [gloss.io :only [encode decode]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [masai.cursor :as cursor]
            [jiraph.graph :as graph]
            [cereal.core :as cereal]
            [jiraph.codecs :as codecs]
            [schematic.core :as schema]
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
        [below above] (split-with #(path-prefix? path (first %) true)
                                  path-to-root)]
    (concat below (take 1 above))))

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

(defn- seq-fn [layer path codecs f]
  (when-let [[id & keys] (seq path)]
    (let [expected-path `[~@keys :*]
          db (:db layer)]
      (when-let [[path codec] (some (validator (fn [[path codec]]
                                                 (= expected-path path)))
                                    codecs)]
        (let [{:keys [start stop keyfn]} (bounds path)
              start (str id ":" start)
              stop (str id ":" stop)
              db-key (fn [k] (str start k))
              read (fn [nodes]
                     (for [[k v] nodes]
                       (map-entry (keyfn k)
                                  (decode codec [(ByteBuffer/wrap v)]))))]
          (condp = f
            seq (fn []
                  (read (db/fetch-subseq db >= start < stop)))
            subseq (fn
                     ([test key]
                        (read (if (#{< <=} test)
                                (db/fetch-subseq db >= start test (db-key key))
                                (db/fetch-subseq db test (db-key key) < stop))))
                     ([start-test start-key end-test end-key]
                        (read (db/fetch-subseq db
                                               start-test (db-key start-key)
                                               end-test (db-key end-key)))))
            rseq (fn []
                   (read (db/fetch-rsubseq db >= start < stop)))
            rsubseq (fn
                      ([test key]
                         (read (if (#{> >=} test)
                                 (db/fetch-rsubseq db test (db-key key) < stop)
                                 (db/fetch-rsubseq db >= start test (db-key key)))))
                      ([start-test start-key end-test end-key]
                         (read (db/fetch-rsubseq db
                                                 start-test (db-key start-key)
                                                 end-test (db-key end-key)))))
            nil))))))

(defn- codecs-for
  "Look up the layout and codecs to use based on node id and revision."
  [layer node-id revision]
  (let [path-fn (get layer (cond (= node-id (meta-key layer "_layer")) :layer-meta-format
                                    (meta-key? layer node-id) :node-meta-format
                                    :else :node-format))]
    (path-fn {:id node-id, :revision revision})))

(defn- delete-ranges!
  "Given a layer and a sequence of [start, end) intervals, delete every key in range. If the
   layer is in append-only mode, then a codec-fn must be included with each interval to enable
   us to encode a reset."
  [layer deletion-ranges]
  (doseq [{:keys [start stop codec]} deletion-ranges]
    (let [delete (if (:append-only? layer)
                   (let [deleted (delay (bufseq->bytes (encode (codecs/special-codec codec :reset)
                                                               {})))]
                     (fn [cursor]
                       (-> cursor
                           (cursor/append (force deleted))
                           (cursor/next))))
                   cursor/delete)]
      (loop [cur (db/cursor (:db layer) start)]
        (when-let [k ^bytes (cursor/key cur)]
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
    (let [[path codec] (first path-codecs)]
      (when (= f (:reduce-fn (meta codec))) ;; performing optimized function
        (let [[id & keys] keyseq]
          (when (= (count keys) (count path)) ;; at exactly this level
            (let [db (:db layer)
                  db-key (db-name keyseq)] ;; great, we can optimize it
              (fn [arg] ;; TODO can we handle multiple args here? not sure how to encode that
                (db/append! db db-key (bufseq->bytes (encode codec arg)))
                {:old nil, :new nil} ;; we didn't read the old data, so we don't know the new data
                ))))))))

(letfn [(fix-incoming [val-fn node layer]
          (into {}
                (for [[id attrs] node]
                  [id (if (and id (meta-key? layer id) ;; is there anything under the :incoming key?
                               (seq (:incoming attrs)))
                        (update-in attrs [:incoming] map-vals val-fn)
                        attrs)])))]
  (def ^:private mapify-incoming    (partial fix-incoming (complement :deleted)))
  (def ^:private structify-incoming (partial fix-incoming (fn [exists]
                                                            {:deleted (not exists)}))))

(defn- write-paths! [layer write-fn codecs id node include-deletions?]
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
          (-> node
              (structify-incoming layer)
              (get id))
          codecs))

(defn- simple-writer [layer path-codecs keyseq f]
  (let [{:keys [db append-only?]} layer
        [id & keys] keyseq
        path-codecs (if append-only?
                      (for [[path codec] path-codecs]
                        [path (codecs/special-codec codec :reset)])
                      path-codecs)
        write-mode (if append-only?, db/append! db/put!)
        writer (partial write-mode db)
        deletion-ranges (for [[path codec] path-codecs
                              :let [{:keys [start stop multi]} (bounds (cons id path))]
                              :when (and multi (prefix-of? (butlast path) keys))]
                          (keyed [start stop codec]))]
    (fn [& args]
      (let [old (read-node path-codecs db id nil)
            new (apply update-in old keyseq f args)]
        (returning {:old (get-in old keyseq), :new (get-in new keyseq)}
          (delete-ranges! layer deletion-ranges)
          (write-paths! layer writer path-codecs id new true))))))

(defmulti specialized-writer
  "If your update function has special semantics that allow it to be distributed over multiple
   paths more efficiently than reading the whole node, applying the function, and then writing to
   each codec, you can implement a method for specialized-writer. For example, useful.utils/adjoin
   can be split up by matching up the paths in the adjoin-arg and in the path-codecs.

   path-codecs is a sequence of [path, codec] pairs, which are computed for your convenience:
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
  (when (every? (fn [[path codec]] ;; TODO support any reduce-fn
                  (= adjoin (:reduce-fn (meta codec))))
                path-codecs)
    (let [db (:db layer)
          [id & keys] keyseq
          writer (partial db/append! db)]
      (fn [arg]
        (returning {:old nil, :new arg}
          (write-paths! layer writer path-codecs id
                        (assoc-in {} keyseq arg)
                        false)))))) ;; don't include deletions


;;; TODO pull the three formats into a single field?
(defrecord MasaiSortedLayer [db revision max-written-revision append-only? node-format node-meta-format layer-meta-format]
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
    (let [node (read-node (codecs-for this id (revision-to-read this)) db id not-found)]
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
          codecs (subnode-codecs (codecs-for this id (revision-to-read this)) keys)]
      (or (seq-fn this keyseq codecs f)
          (if (seq codecs)
            (fn [& args]
              ;; incoming protobufs store their incoming edges with each key pointing to a map like:
              ;; {:deleted bool, :revisions [...], :codec_length ...}.
              ;; we need to coerce it into a map with one entry for each key, whose value is a
              ;; boolean indicating existence (so inverting the meaning of :deleted, and "lifting" it
              ;; outside of the map).
              (let [node (-> (read-node codecs db id nil)
                             (mapify-incoming this))]
                (apply f (get-in node keyseq)
                       args)))
            ;; if no codecs apply, every read will be nil
            (fn [& args] (apply f nil args))))))
  (update-fn [this keyseq f]
    (when-let [[id & keys] (seq keyseq)]
      (let [path-codecs (subnode-codecs (codecs-for this id revision) keys)]
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
    (db/truncate! db)
    (swap! (:max-written-revision layer)
           (constantly nil)))

  Schema
  (schema [this id]
    (reduce (fn [acc [path codec]]
              (let [path (vec path)
                    schema (:schema (meta codec))
                    [path schema] (if (= :* (peek path))
                                    [(pop path) {:type :map
                                                 :keys {:type :string}
                                                 :values schema}]
                                    [path schema])]
                (schema/assoc-in acc path schema)))
            {},
            (reverse (codecs-for this id nil)))) ;; start with shortest path for schema
  (verify-node [this id attrs]
    (try
      ;; do a fake write (does no I/O), to see if an exception would occur
      (do (write-paths! this (constantly nil), (codecs-for this id revision),
                        id, attrs, false)
          true)
      (catch Exception _ false)))

  ChangeLog
  (get-revisions [this id]
    (let [path-codecs (codecs-for this id (revision-to-read this))
          revision-codecs (for [[path codec] path-codecs
                                :let [codec (codecs/special-codec codec :revisions)]
                                :when codec]
                            [path codec])
          revs (->> (node-chunks revision-codecs db id)
                    (tree-seq (any map? sequential?) (to-fix map? vals,
                                                             sequential? seq))
                    (filter number?)
                    (into (sorted-set)))]
      (seq (if revision
             (subseq revs <= revision)
             revs))))

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

(def default-codec (with-meta (cereal/clojure-codec :repeated true)
                     {:reduce-fn adjoin}))

(defn wrap-default-codecs [layout-fn]
  (fn [opts]
    (when-let [layout (layout-fn opts)]
      (for [[path codec] layout]
        [path (or codec default-codec)]))))

(defn wrap-revisioned [layout-fn]
  (fn [opts]
    (when-let [layout (layout-fn opts)]
      (for [[path codec] layout]
        [path ((codecs/revisioned-codec (constantly codec)
                                        (:reduce-fn (meta codec)))
               opts)]))))

(defn make [db & {{:keys [node meta layer-meta]} :formats,
                  :keys [assoc-mode] :or {assoc-mode :append}}]
  (let [[node-format meta-format layer-meta-format]
        (for [format [node meta layer-meta]]
          (condp invoke format
              nil? (wrap-revisioned (constantly [[[:edges :*] default-codec]
                                                 [         [] default-codec]]))
              (! fn?) (constantly format)
              format))]
    (MasaiSortedLayer. (make-db db) nil (atom nil)
                       (case assoc-mode
                         :append true
                         :overwrite false)
                       node-format, meta-format, layer-meta-format)))

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
