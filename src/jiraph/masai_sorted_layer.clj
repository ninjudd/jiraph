(ns jiraph.masai-sorted-layer
  (:use [jiraph.layer :as layer
         :only [SortedEnumerate Optimized Basic Layer ChangeLog Preferences Schema node-id-seq]]
        [jiraph.utils :only [meta-id meta-id? meta-str? base-id keyseq->str]]
        [jiraph.codex :only [encode decode]]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        [useful.utils :only [invoke if-ns adjoin returning map-entry empty-coll? copy-meta switch
                             verify]]
        [useful.seq :only [find-with prefix-of? find-first glue]]
        [useful.string :only [substring-after substring-before]]
        [useful.map :only [assoc-in* map-vals keyed]]
        [useful.fn :only [as-fn knit any fix to-fix ! validator]]
        [useful.io :only [long->bytes bytes->long]]
        [useful.datatypes :only [assoc-record]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
            [masai.cursor :as cursor]
            [jiraph.graph :as graph]
            [cereal.core :as cereal]
            [jiraph.formats :as formats]
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

(defn- subnode-formats
  "This is used to find, given a path through a node and a sequence of [path,format] pairs,
   all the formats that will actually be needed to write at that path. Basically, this means all
   formats whose path is below the write-path, and one format which is at or above it."
  [layout path]
  (let [path-to-root (filter #(along-path? (first %) path) layout)
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

(let [char-after (fn [c]
                   (char (inc (int c))))
      after-colon (char-after \:)
      str-after (fn [s] ;; the string immediately after this one in lexical order
                  (str s \u0001))]
  (defn bounds [path]
    (let [path  (vec path)
          last  (peek path)
          path  (pop path)
          end   (keyseq->str [last])
          start (keyseq->str path)]
      (into {:parent path}
            (if (empty? path) ; top-level
              {:start end
               :stop  (str-after end)
               :keyfn (constantly last)}
              (if (= :* last) ; multi
                {:start (str start ":")
                 :stop  (str start after-colon)
                 :keyfn (substring-after ":")
                 :multi true}
                (let [start-key (str start ":" (name last))]
                  {:start start-key
                   :stop  (str-after start-key)
                   :keyfn (constantly last)})))))))

(defn- find-format [keyseq formats]
  (when-let [expected-path (next keyseq)]
    (some (fn [[path format]]
            (when (= expected-path path)
              format))
          formats)))

(defn- seq-fn [layer keys layout not-found f]
  (let [path `[~@keys :*]]
    (when-let [{:keys [codec]} (find-format path layout)]
      (let [{:keys [start stop keyfn]} (bounds path)
            db-key (partial str start)
            db     (:db layer)
            all    (db/fetch-subseq db >= start < stop)]
        (when-let [fetch-nodes
                   (switch f
                     seq  #(seq all)
                     rseq #(db/fetch-rsubseq db >= start < stop)
                     subseq (fn
                              ([test key]
                                 (if (#{< <=} test)
                                   (db/fetch-subseq db >= start test (db-key key))
                                   (db/fetch-subseq db test (db-key key) < stop)))
                              ([start-test start-key end-test end-key]
                                 (db/fetch-subseq db
                                                  start-test (db-key start-key)
                                                  end-test (db-key end-key))))
                     rsubseq (fn
                               ([test key]
                                  (if (#{> >=} test)
                                    (db/fetch-rsubseq db test (db-key key) < stop)
                                    (db/fetch-rsubseq db >= start test (db-key key))))
                               ([start-test start-key end-test end-key]
                                  (db/fetch-rsubseq db
                                                    start-test (db-key start-key)
                                                    end-test (db-key end-key)))))]
          (fn [& args]
            (if (seq all)
              (for [[k v] (apply fetch-nodes args)]
                (map-entry (keyfn k)
                           (decode codec v)))
              (apply f not-found args))))))))

(defn- layout-for
  "Look up the layout and formats to use based on node id and revision."
  [layer node-id revision]
  (let [layout-fn (get layer (cond (= node-id (meta-id :layer)) :layer-meta-layout-fn
                                   (meta-id? node-id)          :node-meta-layout-fn
                                   :else                        :node-layout-fn))]
    (layout-fn {:id node-id, :revision revision})))

(defn- delete-ranges!
  "Given a layer and a sequence of [start, end) intervals, delete every key in range. If the
   layer is in append-only mode, then a format must be included with each interval to enable
   us to encode a reset."
  [layer deletion-ranges]
  (doseq [{:keys [start stop format]} deletion-ranges]
    (let [delete (if (:append-only? layer)
                   (let [deleted (delay (encode (formats/special-codec format :reset)
                                                {}))]
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

(defn- node-chunks [layout db id]
  (for [[path {:keys [codec]}] layout
        :let [{:keys [start stop parent keyfn]} (bounds (cons id path))
              kvs (seq (for [[k v] (db/fetch-seq db start)
                             :while (neg? (compare k stop))
                             :let [node (decode codec v)]
                             :when (not (empty-coll? node))]
                         (map-entry (keyfn k) node)))]
        :when kvs]
    (assoc-in* {} parent
               (into {} kvs))))

(letfn [(fix-incoming [val-fn nodes]
          (into {}
                (for [[id attrs] nodes]
                  [id (if (and id (meta-id? id) ;; is there anything under the :incoming key?
                               (seq (:incoming attrs)))
                        (update-in attrs [:incoming] map-vals val-fn)
                        attrs)])))]
  (def ^:private mapify-incoming    (partial fix-incoming (complement :deleted)))
  (def ^:private structify-incoming (partial fix-incoming (fn [exists]
                                                            {:deleted (not exists)}))))

(defn- read-node [layout db id not-found]
  (-> (reduce (fn ;; for an empty list (no keys found), reduce calls f with no args
                ([] not-found)
                ([a b] (adjoin a b)))
              (node-chunks layout db id))
      (fix #(not (identical? not-found %))
           mapify-incoming)))

(defn- optimized-writer
  "Return a writer iff the keyseq corresponds exactly to one path in path-formats, and the
   corresponding format has f as its reduce-fn (that is, we can apply the change to f by merely
   appending something to a single database value)."
  [layer layout keyseq f]
  (when-not (next layout) ;; can only optimize a single codec
    (let [[path format] (first layout)]
      (when (= f (:reduce-fn format)) ;; performing optimized function
        (let [[id & keys] keyseq]
          (when (= (count keys) (count path)) ;; at exactly this level
            (let [db  (:db layer)
                  key (keyseq->str keyseq)] ;; great, we can optimize it
              (fn [arg] ;; TODO can we handle multiple args here? not sure how to encode that
                (db/append! db key (encode (:codec format) arg))
                ;; we didn't read the old data, so we don't know the new data
                {:old nil, :new nil}))))))))

(defn- write-paths! [layer write-fn layout id node include-deletions? codec-type]
  (reduce (fn [node [path format]]
            (let [write! (fn [key data]
                           (->> data
                                (encode (get format codec-type))
                                (write-fn key)))]
              (reduce (fn [node path]
                        (let [path (vec path)
                              data (get-in node path)]
                          (write! (keyseq->str (cons id path)) data)
                          (when (seq path)
                            (no-nil-update node (pop path) dissoc (peek path)))))
                      node (matching-subpaths node path include-deletions?))))
          (-> (structify-incoming node)
              (get id))
          layout))

(defn- simple-writer [layer layout keyseq f]
  (let [{:keys [db append-only?]} layer
        [id & keys] keyseq
        [write-mode codec-type] (if append-only?
                                  [db/append! :reset]
                                  [db/put! :codec])
        writer (partial write-mode db)
        deletion-ranges (for [[path format] layout
                              :let [{:keys [start stop multi]} (bounds (cons id path))]
                              :when (and multi (prefix-of? (butlast path) keys))]
                          (keyed [start stop format]))]
    (fn [& args]
      (let [old (read-node layout db id nil)
            new (apply update-in old keyseq f args)]
        (returning {:old (get-in old keyseq), :new (get-in new keyseq)}
          (delete-ranges! layer deletion-ranges)
          (write-paths! layer writer layout id new true codec-type))))))

(defmulti specialized-writer
  "If your update function has special semantics that allow it to be distributed over multiple
   paths more efficiently than reading the whole node, applying the function, and then writing to
   each format, you can implement a method for specialized-writer. For example, useful.utils/adjoin
   can be split up by matching up the paths in the adjoin-arg and in the layout.

   layout is a sequence of [path, format] pairs, which are computed for your convenience:
   if you preferred, you could recalculate them from layer and keyseq.
   You should return a function of one argument, which writes to the layer the result of
   (update-in layer keyseq f arg). See the contract for jiraph.layer/update-fn - you are
   effectively implementing an update-fn for a particular function rather than a layer.

   Note that it is acceptable to return nil instead of a function, if you find the keyseq or
   layout means you cannot do any optimization."
  (fn [layer layout keyseq f]
    f))

(defmethod specialized-writer :default [& args]
  nil)

(defmethod specialized-writer adjoin [layer layout keyseq _]
  (when (every? (fn [[path format]] ;; TODO support any reduce-fn
                  (= adjoin (:reduce-fn format)))
                layout)
    (let [db (:db layer)
          [id & keys] keyseq
          writer (partial db/append! db)]
      (fn [arg]
        (returning {:old nil, :new arg}
          (write-paths! layer writer layout id
                        (assoc-in {} keyseq arg)
                        false :codec)))))) ;; don't include deletions

;;; TODO pull the three formats into a single field?
(defrecord MasaiSortedLayer [db revision max-written-revision append-only?
                             node-layout-fn node-meta-layout-fn layer-meta-layout-fn]
  SortedEnumerate
  (node-id-subseq [this cmp start]
    (verify (#{> >=} cmp) "Only > and >= supported")
    (let [start (if (= cmp >)
                  (str start ";")
                  start)
          entries (db/fetch-subseq db >= start)
          ids (map first entries)]
      (glue (fn [old new] new)
            =
            (constantly false)
            (map (substring-before ":")
                 (remove meta-str? ids)))))
  (node-subseq [this cmp start]
    (for [id (layer/node-id-subseq this cmp start)]
      (map-entry id (graph/get-node this id))))

  Basic
  (get-node [this id not-found]
    (let [node (read-node (layout-for this id (revision-to-read this)) db id not-found)]
      (if (identical? node not-found)
        not-found
        (get node id))))
  (assoc-node! [this id attrs]
    ((layer/update-fn this [id] (constantly attrs)))
    true)
  (dissoc-node! [this id]
    (db/delete! db id))

  Optimized
  (query-fn [this keyseq not-found f]
    (let [[id & keys] keyseq
          layout (subnode-formats (layout-for this id (revision-to-read this)) keys)]
      (or (seq-fn this keyseq layout not-found f)
          (if (seq layout)
            (fn [& args]
              ;; incoming protobufs store their incoming edges with each key pointing to a map like:
              ;; {:deleted bool, :revisions [...], :codec_length ...}.
              ;; we need to coerce it into a map with one entry for each key, whose value is a
              ;; boolean indicating existence (so inverting the meaning of :deleted, and "lifting" it
              ;; outside of the map).
              (let [node (read-node layout db id not-found)]
                (apply f (if (= node not-found)
                           not-found
                           (get-in node keyseq))
                       args)))
            ;; if no codecs apply, every read will be nil
            (fn [& args] (apply f not-found args))))))
  (update-fn [this keyseq f]
    (when-let [[id & keys] (seq keyseq)]
      (let [layout (subnode-formats (layout-for this id revision) keys)]
        (assert (seq layout) "No codecs to write with")
        (some #(% this layout keyseq f)
              [specialized-writer, optimized-writer, simple-writer]))))

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
    (reset! (:max-written-revision this) nil))

  Schema
  (schema [this id]
    (reduce (fn [acc [path format]]
              (let [path (vec path)
                    schema (:schema format)
                    [path schema] (if (= :* (peek path))
                                    [(pop path) {:type :map
                                                 :keys {:type :string}
                                                 :values schema}]
                                    [path schema])]
                (schema/assoc-in acc path schema)))
            {},
            (reverse (layout-for this id nil)))) ;; start with shortest path for schema
  (verify-node [this id attrs]
    (try
      ;; do a fake write (does no I/O), to see if an exception would occur
      (do (write-paths! this (constantly nil), (layout-for this id revision),
                        id, attrs, false, :codec)
          true)
      (catch Exception _ false)))

  ChangeLog
  (get-revisions [this id]
    (let [layout (layout-for this id (revision-to-read this))
          revisioned-layout (for [[path format] layout
                                  :let [codec (:revisions format)]
                                  :when codec] ; pretend :revisions is the normal codec, so that
                                               ; node-chunks will read using it
                              [path {:codec codec}])
          revs (->> (node-chunks revisioned-layout db id)
                    (tree-seq (any map? sequential?) (to-fix map? vals,
                                                             sequential? seq))
                    (filter number?)
                    (into (sorted-set)))]
      (seq (if revision
             (subseq revs <= revision)
             revs))))

  ;; TODO this is stubbed, will need to work eventually
  (get-changed-ids [this rev]
    #{})

  ;; TODO: Problems with implicit transaction-wrapping: we end up writing that the revision has
  ;; been applied, and refusing to do any more writing to that revision. What is the answer?
  WrappedTransactional
  (txn-wrap [this f]
    ;; todo *skip-writes* for past revisions
    (fn [^MasaiSortedLayer layer]
      (let [db-wrapped (txn-wrap db ; let db wrap transaction, but call f with layer
                                 (fn [_]
                                   (try
                                     (returning (f layer)
                                       (save-maxrev layer))
                                     (catch Throwable t ; something went wrong, so we need to toss
                                                        ; out our cached revision and rely on the
                                                        ; one stored on disk.
                                       (reset! (.max-written-revision layer) nil)
                                       (throw t)))))]
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

(def default-format {:codec (cereal/clojure-codec :repeated true)
                     :reduce-fn adjoin})

(defn wrap-default-formats [layout-fn]
  (fn [opts]
    (when-let [layout (layout-fn opts)]
      (for [[path format] layout]
        [path (merge default-format format)]))))

(defn wrap-revisioned [layout-fn]
  (fn [opts]
    (when-let [layout (layout-fn opts)]
      (for [[path format] layout]
        [path ((formats/revisioned-format (constantly format))
               opts)]))))

(if-ns (:require [masai.tokyo-sorted :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

(defn make [db & {{:keys [node meta layer-meta]} :layout-fns,
                  :keys [assoc-mode] :or {assoc-mode :append}}]
  (let [[node-fn meta-fn layer-meta-fn]
        (for [layout-fn [node meta layer-meta]]
          (condp invoke layout-fn
            ;; this is not the correct default for meta and layer-meta
            nil? (wrap-revisioned (constantly [[[:edges :*] default-format]
                                               [         [] default-format]]))
            (! fn?) (constantly layout-fn)
            layout-fn))]
    (MasaiSortedLayer. (make-db db) nil (atom nil)
                       (case assoc-mode
                         :append true
                         :overwrite false)
                       node-fn, meta-fn, layer-meta-fn)))

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
