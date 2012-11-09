(ns jiraph.masai-sorted-layer
  (:use [jiraph.layer :as layer
         :only [SortedEnumerate Optimized Basic Layer ChangeLog Schema node-id-seq]]
        [jiraph.utils :only [keyseq->str meta-str?]]
        [jiraph.codex :as codex :only [encode decode]]
        [jiraph.masai-common :only [implement-ordered revision-to-read revision-key?]]
        [retro.core :only [Transactional Revisioned OrderedRevisions
                           txn-begin! txn-commit! txn-rollback!]]
        [useful.utils :only [if-ns adjoin returning empty-coll? switch]]
        [useful.seq :only [prefix-of? single? remove-prefix glue take-until assert-length]]
        [useful.state :only [volatile put!]]
        [useful.map :only [update assoc-in* merge-in keyed]]
        [useful.fn :only [as-fn any to-fix]]
        [useful.io :only [compare-bytes]]
        [useful.datatypes :only [assoc-record]]
        useful.debug)
  (:require [flatland.masai.db :as db]
            [flatland.masai.cursor :as cursor]
            [jiraph.graph :as graph :refer [with-action]]
            [cereal.core :as cereal]
            [jiraph.formats :as formats]
            [schematic.core :as schema]
            [clojure.string :as s])
  (:import [java.nio ByteBuffer]))

(defn- layout
  [mode layer id]
  (let [revision (case mode
                   :read (revision-to-read layer)
                   :write (:revision layer))]
    ((:layout-fn layer) (keyed [id revision]))))

(defn path-match
  "Try to match pattern to keyseq. If pattern matches keyseq, return a map containing
  the matching :prefix and the remaining :suffix or :pattern, whichever is left."
  [keyseq pattern]
  (loop [pattern (cons :* pattern)
         suffix (sequence keyseq)
         prefix []]
    (if (seq pattern)
      (let [x (first pattern)]
        (if (seq suffix)
          (let [y (first suffix)]
            (when (or (= x y) (= x :*))
              (recur (rest pattern)
                     (rest suffix)
                     (conj prefix y))))
          (keyed [prefix pattern])))
      (keyed [prefix suffix]))))

(defn matching-path [keyseq]
  (fn [layout-entry]
    (when-let [match (path-match keyseq (:pattern layout-entry))]
      (assoc layout-entry :match match))))

(defn full-match? [path-match]
  (and path-match
       (every? #{:*} (:pattern path-match))))

(defn wildcard-match? [path-match]
  (= :* (first (:pattern path-match))))

(defn along-path?
  "Is either of these a path-prefix of the other?"
  [pattern keyseq]
  (boolean (path-match keyseq pattern)))

(defn match-path?
  "Does pattern match path exactly?"
  [pattern keyseq]
  (= [] (:suffix (path-match keyseq pattern))))

(defn find-codec
  "Find the first codec of type codec-type in layout that matches keyseq."
  [layout codec-type keyseq]
  (when-let [{:keys [format]} (first (filter #(match-path? (:pattern %) keyseq)
                                             layout))]
    (or (get format codec-type)
        (get format :codec))))

(defn codec-finder
  "Returns a function to find the first [path, format] pair in the applicable layout that matches
  keyseq exactly."
  [layer mode codec-type]
  (let [layout-fn (memoize (fn [id] (layout mode layer id)))]
    (fn [keyseq]
      (-> (layout-fn (first keyseq))
          (find-codec codec-type keyseq)))))

(defn- subnode-layout
  "This is used to find, given mode (:read|:write) a layer and a keyseq, the subset of the layout
   that will actually be needed for that keyseq. Basically, this means all formats matching that
   keyseq up until the first format that matches the keyseq completely."
  [mode layer keyseq]
  (when (seq keyseq)
    (->> (layout mode layer (first keyseq))
         (keep (matching-path keyseq))
         (take-until (comp full-match? :match)))))

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

(defn inc-last-byte [^bytes b]
  (let [index (dec (alength b))]
    (when-not (neg? index)
      (aset b index
            (->> (aget b index)
                 (unchecked-inc)
                 (unchecked-byte)))
      b)))

(defn bounds
  "Compute the bounds for fetching db records under prefix. Valid options are:
    :start-test, :start, :end-test, :end
   These options correspond to the arguments to clojure.core's subseq and rsubseq."
  ([key-codec prefix]
     (bounds key-codec prefix {}))
  ([key-codec prefix opts]
     (let [{:keys [start end start-test end-test]
            :or {start-test >=, end-test <=}} opts]
       (if (contains? #{< <=} start-test)
         (bounds key-codec prefix {:start-test >=
                                   :end-test start-test
                                   :end start})
         (let [[start end] (for [key [start end]]
                             (encode key-codec (if key
                                                 (concat prefix [key])
                                                 prefix)))
               [end end-test] (if (= end-test <=)
                                [(inc-last-byte end) <]
                                [end end-test])]
           (keyed [start end start-test end-test]))))))

(defn fetch
  "Fetch a single single chunk of a node at keyseq, using val-codec to decode. Returns a sparse map
  at the top level of the graph."
 [layer keyseq val-codec]
  (let [{:keys [key-codec db]} layer
        key (encode key-codec keyseq)]
    (when-let [val (db/fetch db key)]
      (->> (decode val-codec val)
           (assoc-in* {} keyseq)))))

(defn- fetch-range
  "Fetch a range of node chunks beneath prefix. Returns a seq of sparse maps at the top level of the
  graph. Valid options are:
   :reverse?    - fetch chunks in reverse
   :codec-type  - which codec type to use for decoding values (default :codec)
   :layout      - layout to use for looking up codecs (optimization)
   :start-test, :start, :end-test, :end - options passed to bounds"
  [layer prefix opts]
  (let [{:keys [key-codec db]} layer
        {:keys [start end start-test end-test]} (bounds key-codec prefix opts)
        {:keys [reverse? codec-type layout] :or {codec-type :codec}} opts
        fetch (if reverse? db/fetch-rsubseq db/fetch-subseq)
        codec (if layout
                (partial find-codec layout codec-type)
                (codec-finder layer :read codec-type))]
    (for [[key val] (apply fetch db
                           start-test start
                           (when end [end-test end]))
          :when (not (revision-key? key))
          :let [keyseq (decode key-codec key)
                val-codec (codec keyseq)]
          :when val-codec
          :let [value (decode val-codec val)]
          :when (not (empty-coll? value))]
      (assoc-in* {} keyseq value))))

(defn node-chunks
  "Fetch all chunks necessary to assemble a node or subnode at keyseq. Use codec-type for
  decoding. Returns a seq of sparse maps at the top level of the graph."
  [layer codec-type keyseq]
  (apply concat
         (for [[prefix layout] (group-by (comp :prefix :match)
                                         (subnode-layout :read layer keyseq))]
           (if (or (next layout)
                   (:pattern (-> layout first :match)))
             (fetch-range layer prefix (keyed [codec-type layout]))
             (let [val-codec (get-in (first layout) [:format codec-type])]
               [(fetch layer prefix val-codec)])))))

(defn- get-node-seq
  "Return a sequence of nodes or subnodes at prefix depth. Takes the same opts as fetch-range."
  [layer prefix opts]
  (->> (fetch-range layer prefix opts)
       (map #(get-in % prefix))
       (glue merge-in {} #(= (keys %1) (keys %2)))))

(defn- get-in-node
  "Return the node or subnode at keyseq. Returns not-found if the keyseq is not present."
  [layer keyseq not-found]
  {:pre [(seq keyseq)]}
  (if-let [chunks (seq (node-chunks layer :codec keyseq))]
    (-> (reduce merge-in chunks)
        (get-in keyseq not-found))
    not-found))

(defn- subseq-fn [node-entries-fn opts]
  (fn
    ([start-test start]
       (node-entries-fn (merge opts (keyed [start-test start]))))
    ([start-test start end-test end]
       (node-entries-fn (merge opts (keyed [start-test start end-test end]))))))

(defn seq-fn [layer keyseq not-found f]
  (when (some (comp wildcard-match? :match)
              (subnode-layout :read layer keyseq))
    (let [node-entries #(mapcat seq (get-node-seq layer keyseq %))]
      (when-let [node-subseq (switch f
                               seq #(node-entries {})
                               rseq #(node-entries {:reverse? true})
                               subseq (subseq-fn node-entries {})
                               rsubseq (subseq-fn node-entries {:reverse? true}))]
        (fn [& args]
          (or (seq (apply node-subseq args))
              (when (empty? (node-entries {}))
                (apply f not-found args))))))))

(defn- optimized-writer
  "Return a writer iff the keyseq corresponds exactly to one path in path-formats, and the
   corresponding format has f as its reduce-fn (that is, we can apply the change to f by merely
   appending something to a single database value)."
  [layer layout keyseq f args]
  (when-not (next layout) ;; can only optimize a single codec
    (let [{:keys [pattern format]} (first layout)]
      (when (and (= f (:reduce-fn format)) ;; performing optimized function
                 (single? args)) ;; with just one arg
        (let [[id & keys] keyseq]
          (when (= (count keys) (count pattern)) ;; at exactly this level
            (let [db (:db layer)
                  key (encode (:key-codec layer) keyseq)
                  arg (first args)] ;; great, we can optimize it
              (fn [layer']
                (let [[_ format] (first (layout :write layer' id))]
                  (db/append! db key
                              (encode (:codec format) arg)))))))))))

(defn- path-write-fn [include-empty? write-fn codec-type]
  (fn [layer keyseq node]
    (let [{:keys [db key-codec]} layer
          layout (subnode-layout :write layer keyseq)
          id (first keyseq)
          node (assoc-in* {} keyseq node)]
      (reduce (fn [node {:keys [pattern format match]}]
                (reduce (fn [node path]
                          (let [path (vec path)]
                            (write-fn db
                                      (encode key-codec (cons id path))
                                      (encode (get format codec-type)
                                              (get-in node path)))
                            (when (seq path)
                              (no-nil-update node (pop path) dissoc (peek path)))))
                        node
                        (matching-subpaths node pattern include-empty?)))
              (get node id)
              layout))))

(defn- delete-range!
  "Given a layer and a prefix, delete every key beneath that prefix."
  [layer prefix & {:keys [delete-exact?]}]
  (when (seq prefix)
    (let [{:keys [key-codec append-only?]} layer
          {:keys [start end]} (bounds key-codec prefix)
          reset-codec (codec-finder layer :write :reset)
          encode (memoize encode)]
      (loop [cur (db/cursor (:db layer) start)]
        (when cur
          (when-let [^bytes key (cursor/key cur)]
            (when (neg? (compare-bytes key end))
              (recur (if append-only?
                       (let [keyseq (decode key-codec key)]
                         (when (or delete-exact? (not= keyseq prefix))
                           (cursor/append cur (->> {}
                                                   (encode (reset-codec keyseq)))))
                         (cursor/next cur))
                       (cursor/delete cur))))))))))

(defn- bounds-match? [{:keys [multi parent]} keyseq]
  (and multi
       (prefix-of? parent keyseq)))

(defn- simple-writer [layer layout keyseq f args]
  (let [{:keys [db append-only?]} layer
        [id & keys] keyseq
        key-codec (:key-codec layer)
        write! (apply path-write-fn true
                      (if append-only?
                        [db/append! :reset]
                        [db/put! :codec]))]
    (fn [layer']
      (let [old (get-in-node layer' keyseq nil)
            new (apply f old args)]
        (delete-range! layer' keyseq)
        (write! layer' keyseq new)))))

(defmulti specialized-writer
  "If your update function has special semantics that allow it to be distributed over multiple
   paths more efficiently than reading the whole node, applying the function, and then writing to
   each format, you can implement a method for specialized-writer. For example, useful.utils/adjoin
   can be split up by matching up the paths in the adjoin-arg and in the layout.

   layout is a sequence of [path, format] pairs, which are computed for your convenience: if you
   preferred, you could recalculate them from layer and keyseq.  You should return a retro IOValue
   for writing to the layer the result of (apply update-in layer keyseq f args). See the contract
   for jiraph.layer/update-in-node - you are effectively implementing an updater for a particular
   function rather than a layer.

   Note that it is acceptable to return nil instead of a function, if you find the keyseq or
   layout means you cannot do any optimization."
  (fn [layer layout keyseq f args]
    f))

(defmethod specialized-writer :default [& args]
  nil)

(defmethod specialized-writer adjoin [layer layout keyseq _ args]
  (when (and (every? (fn [{:keys [format]}] ;; TODO support any reduce-fn
                       (= adjoin (:reduce-fn format)))
                     layout)
             (single? args)) ;; can only adjoin with exactly one arg
    (let [db (:db layer)
          [id & keys] keyseq
          write! (path-write-fn false db/append! :codec)
          arg (first args)]
      (fn [layer']
        (write! layer' keyseq arg)))))

(defrecord MasaiSortedLayer [db revision max-written-revision append-only? layout-fn key-codec]
  SortedEnumerate
  ;; TODO implement these
  (node-id-subseq [layer opts])
  (node-subseq [layer opts])

  Basic
  (get-node [this id not-found]
    (get-in-node this [id] not-found))
  (update-in-node [this keyseq f args]
    (let [ioval (graph/simple-ioval this keyseq f args)]
      (if (seq keyseq)
        (let [layout (subnode-layout :read this keyseq)]
          (assert (seq layout) "No codecs to write with")
          (ioval (some #(% this layout keyseq f args)
                       [specialized-writer optimized-writer simple-writer])))
        (condp = f
          dissoc (let [[node-id] (assert-length 1 args)]
                   (ioval (fn [layer']
                            (delete-range! layer' [node-id] :delete-exact? true))))
          assoc (let [[node-id attrs] (assert-length 2 args)]
                  (recur [node-id] (constantly attrs) nil))
          (throw (IllegalArgumentException. (format "Can't apply function %s at top level"
                                                    f)))))))

  Optimized
  (query-fn [this keyseq not-found f]
    (or (seq-fn this keyseq not-found f)
        (fn [& args]
          (apply f (get-in-node this keyseq not-found) args))))

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
    (put! max-written-revision nil)
    (db/truncate! db))
  (same? [this other]
    (apply = (for [layer [this other]]
               (get-in layer [:db :opts :path]))))

  Schema
  (schema [this id]
    (reduce (fn [acc {:keys [pattern format]}]
              (let [path (vec pattern)
                    schema (:schema format)
                    [path schema] (if (= :* (peek path))
                                    [(pop path) {:type :map
                                                 :keys {:type :string}
                                                 :values schema}]
                                    [path schema])]
                (schema/assoc-in acc path schema)))
            {},
            (reverse (layout :read this id)))) ;; start with shortest path for schema
  (verify-node [this id attrs]
    ;; TODO remove
    )

  ChangeLog
  (get-revisions [this id]
    (let [revs (->> (node-chunks this :revisions [id])
                    (tree-seq (any map? sequential?)
                              (to-fix map? vals, sequential? seq))
                    (filter number?)
                    (into (sorted-set)))]
      (seq (if revision
             (subseq revs <= revision)
             revs))))

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

(implement-ordered MasaiSortedLayer)

(def default-format {:codec (cereal/clojure-codec :repeated true)
                     :reduce-fn adjoin})

(defn wrap-default-formats [layout-fn]
  (fn [opts]
    (when-let [layout (layout-fn opts)]
      (for [entry layout]
        (merge {:format default-format} entry)))))

(defn revisioned-format [format opts]
  ((formats/revisioned-format (constantly format))
   opts))

(defn wrap-revisioned [layout-fn]
  (fn [opts]
    (when-let [layout (layout-fn opts)]
      (for [entry layout]
        (update entry :format revisioned-format opts)))))

(if-ns (:require [flatland.masai.tokyo-sorted :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

(def simple-key-codec {:read (fn [db-key]
                               (s/split (String. ^bytes db-key) #":"))
                       :write #(.getBytes ^String (s/join ":" (map name %)))})

(def default-key-codec (-> simple-key-codec
                           (codex/wrap identity
                                       (fn [keys]
                                         (if (next keys)
                                           (update-in keys [1] keyword)
                                           keys)))))

(let [default-layout-fn (wrap-revisioned
                         (constantly [{:pattern [:edges :*] :format default-format}
                                      {:pattern []          :format default-format}]))]
  (defn make [db & {:keys [assoc-mode layout-fn key-codec]
                    :or {assoc-mode :append
                         layout-fn default-layout-fn
                         key-codec default-key-codec}}]
    (MasaiSortedLayer. (make-db db) nil (volatile nil)
                       (case assoc-mode
                         :append true
                         :overwrite false)
                       (as-fn layout-fn)
                       key-codec)))

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
