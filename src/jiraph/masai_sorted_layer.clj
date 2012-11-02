(ns jiraph.masai-sorted-layer
  (:use [jiraph.layer :as layer
         :only [SortedEnumerate Optimized Basic Layer ChangeLog Schema node-id-seq]]
        [jiraph.utils :only [keyseq->str meta-str? assert-length]]
        [jiraph.codex :only [encode decode]]
        [jiraph.masai-common :only [implement-ordered revision-to-read]]
        [retro.core :only [Transactional Revisioned OrderedRevisions
                           txn-begin! txn-commit! txn-rollback!]]
        [useful.utils :only [invoke if-ns adjoin returning map-entry empty-coll? switch verify update-peek]]
        [useful.seq :only [find-with prefix-of? single? remove-prefix find-first glue]]
        [useful.state :only [volatile put!]]
        [useful.string :only [substring-after substring-before]]
        [useful.map :only [assoc-in* map-vals keyed]]
        [useful.fn :only [as-fn knit any fix to-fix ! validator decorate]]
        [useful.io :only [long->bytes bytes->long compare-bytes]]
        [useful.datatypes :only [assoc-record]]
        useful.debug
        [io.core :only [bufseq->bytes]])
  (:require [flatland.masai.db :as db]
            [flatland.masai.cursor :as cursor]
            [jiraph.graph :as graph :refer [with-action]]
            [cereal.core :as cereal]
            [jiraph.formats :as formats]
            [schematic.core :as schema]
            [clojure.string :as s])
  (:import [java.nio ByteBuffer]))

(defn- layout
  [kind layer id]
  (let [revision (case kind
                   :read (revision-to-read layer)
                   :write (:revision layer))]
    ((:layout-fn layer) (keyed [id revision]))))

(defn path-match
  "Try to match pattern to keyseq. If pattern matches keyseq, return a map containing
  the matching :prefix and the remaining :suffix or :pattern, whichever is left."
  [pattern keyseq]
  (loop [pattern (cons :* pattern)
         suffix (sequence keyseq)
         prefix []]
    (if (seq pattern)
      (let [x (first pattern)]
        (if (seq suffix)
          (let [y (first suffix)]
            (when (contains? #{y :*} x)
              (recur (rest pattern)
                     (rest suffix)
                     (conj prefix y))))
          (keyed [prefix pattern])))
      (keyed [prefix suffix]))))

(defn full-match? [path-match]
  (every? #{:*} (:pattern path-match)))

(defn split-keyseq
  "Split keyseq using the patterns from the appropriate layout."
  [layer keyseq]
  (if (seq keyseq)
    (let [layout (layout :read layer (first keyseq))]
      (->> layout
           (map #(path-match (first %) keyseq))
           (filter full-match?)
           (first)))
    {:prefix []}))

(defn along-path?
  "Is either of these a path-prefix of the other?"
  [pattern keyseq]
  (boolean (path-match pattern keyseq)))

(defn match-path?
  "Does pattern match path exactly?"
  [pattern keyseq]
  (= [] (:suffix (path-match pattern keyseq))))

(defn codec-finder
  "Returns a function to find the first [path, format] pair in the applicable layout that matches
  keyseq exactly."
  [layer codec-type]
  (let [layout-fn (memoize (fn [id] (layout :read layer id)))]
    (fn [keyseq]
      (let [layout (layout-fn (first keyseq))]
        (when-let [[path format] (first (filter #(match-path? (first %) keyseq)
                                                layout))]
          (or (get format key)
              (get format :codec)))))))

(defn- subnode-layout
  "This is used to find, given a layout and a keyseq, the subset of the layout that will actually be
   needed to read or write at that keyseq. Basically, this means all formats whose path is below the
   keyseq, and one format which is at or above it."
  [kind layer keyseq]
  (when (seq keyseq)
    (let [[below above] (->> (layout kind layer (first keyseq))
                             (map (decorate #(path-match (first %) keyseq)))
                             (filter second)
                             (split-with (comp full-match? second)))]
      (map first (concat below (take 1 above))))))

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
    :start-test :start :end-test :end
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

(defn node-chunks
  ([layer prefix]
     (node-chunks layer prefix {}))
  ([layer prefix opts]
     (let [{:keys [key-codec]} layer
           {:keys [start end start-test end-test]} (bounds key-codec prefix opts)
           {:keys [reverse? codec-type] :or {codec-type :codec}} opts
           fetch (if reverse? db/fetch-rsubseq db/fetch-subseq)
           codec (codec-finder layer codec-type)]
       (glue conj [] #(= (keys %1) (keys %2))
             (for [[key val] (apply fetch (:db layer)
                                    start-test start
                                    (when end [end-test end]))
                   :let [keyseq (decode key-codec key)
                         suffix (remove-prefix prefix keyseq)
                         val-codec (codec keyseq)]
                   :when (and suffix val-codec)]
               (->> (decode val-codec val)
                    (assoc-in* {} suffix)))))))

(defn- subseq-fn [node-entries-fn opts]
  (fn
    ([start-test start]
       (node-entries-fn (merge opts (keyed [start-test start]))))
    ([start-test start end-test end]
       (node-entries-fn (merge opts (keyed [start-test start end-test end]))))))

(defn seq-fn [layer keyseq not-found f]
  (when-not (:suffix (split-keyseq layer keyseq))
    (let [node-entries #(mapcat seq (node-chunks layer keyseq %))
          node-subseq (switch f
                        seq #(node-entries {})
                        rseq #(node-entries {:reverse? true})
                        subseq (subseq-fn node-entries {})
                        rsubseq (subseq-fn node-entries {:reverse? true}))]
      (when node-subseq
        (fn [& args]
          (or (seq (apply subseq args))
              (when (empty? (node-entries {}))
                (apply f not-found args))))))))

(defn- get-in-node [layer keyseq not-found]
  {:pre [(seq keyseq)]}
  (let [{:keys [prefix suffix]} (split-keyseq layer keyseq)]
    (if-let [chunks (seq (node-chunks layer prefix))]
      (-> (reduce adjoin chunks)
          (get-in suffix not-found))
      not-found)))

(defn- optimized-writer
  "Return a writer iff the keyseq corresponds exactly to one path in path-formats, and the
   corresponding format has f as its reduce-fn (that is, we can apply the change to f by merely
   appending something to a single database value)."
  [layer layout keyseq f args]
  (when-not (next layout) ;; can only optimize a single codec
    (let [[path format] (first layout)]
      (when (and (= f (:reduce-fn format)) ;; performing optimized function
                 (single? args)) ;; with just one arg
        (let [[id & keys] keyseq]
          (when (= (count keys) (count path)) ;; at exactly this level
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
      (reduce (fn [node [path format]]
                (reduce (fn [node path]
                          (let [path (vec path)]
                            (write-fn db
                                      (encode key-codec (cons id path))
                                      (encode (get format codec-type)
                                              (get-in node path)))
                            (when (seq path)
                              (no-nil-update node (pop path) dissoc (peek path)))))
                        node
                        (matching-subpaths node path include-empty?)))
              (get node id)
              layout))))

(defn- delete-in-node!
  "Given a layer and a keyseq, delete every key beneath that keyseq. We need the
  node-format in case the layer is in append-only mode so we can encode a reset."
  [layer keyseq]
  (when (seq keyseq)
    (let [{:keys [key-codec append-only?]} layer
          {:keys [start end]} (bounds key-codec keyseq)
          reset-codec (codec-finder layer :reset)
          encode (memoize encode)]
      (loop [cur (db/cursor (:db layer) start)]
        (when cur
          (when-let [^bytes key (cursor/key cur)]
            (when (neg? (compare-bytes key end))
              (recur (if append-only?
                       (let [keyseq (decode key-codec key)
                             deleted (encode (reset-codec keyseq) {})]
                         (-> cur
                             (cursor/append deleted)
                             (cursor/next)))
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
        (delete-in-node! layer' keyseq)
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
  (when (and (every? (fn [[path format]] ;; TODO support any reduce-fn
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
          dissoc (let [[node-id] (assert-length 1 args)] ;; TODO don't think this is right, but copying from old
                   ;; version for now.
                   (ioval (fn [layer']
                            (db/delete! db node-id))))
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
            (reverse (layout :read this id)))) ;; start with shortest path for schema
  (verify-node [this id attrs]
    ;; TODO remove
    )

  ChangeLog
  (get-revisions [this id]
    (let [revs (->> (node-chunks this [id] {:codec-type :revisions})
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
      (for [[path format] layout]
        [path (merge default-format format)]))))

(defn wrap-revisioned [layout-fn]
  (fn [opts]
    (when-let [layout (layout-fn opts)]
      (for [[path format] layout]
        [path ((formats/revisioned-format (constantly format))
               opts)]))))

(if-ns (:require [flatland.masai.tokyo-sorted :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

(let [default-layout-fn (wrap-revisioned (constantly [[[:edges :*] default-format]
                                                      [         [] default-format]]))
      default-key-codec {:read #(s/split (String. ^bytes %) #":")
                         :write #(.getBytes ^String (s/join ":" (map name %)))}]
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
