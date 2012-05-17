(ns jiraph.merge
  (:use jiraph.layer retro.core
        [jiraph.core :only [layer]]
        [jiraph.utils :only [meta-keyseq? meta-id? meta-id base-id]]
        [useful.map :only [map-vals-with-keys update dissoc-in* assoc-in* update-in*]]
        [useful.seq :only [merge-sorted indexed]]
        [useful.fn :only [fix fixing given]]
        [useful.utils :only [adjoin verify]]
        [useful.datatypes :only [assoc-record]]
useful.debug
        [ego.core :only [type-key]]
        [clojure.core.match :only [match]])
  (:require [jiraph.graph :as graph]))

(declare merge-ids merge-head merge-position node-deleted?)

(defn- edges-keyseq [keyseq]
  (if (meta-keyseq? keyseq)
    (match keyseq
      [_]           [:incoming]
      [_ :incoming] [])
    (match keyseq
      [_]        [:edges]
      [_ :edges] [])))

(defn- merge-edges [id-layer keyseq edges-seq]
  (let [incoming? (meta-keyseq? keyseq)
        deleted?  (if incoming? not :deleted)]
    (letfn [(edge-sort-order [i id edge]
              (let [pos (merge-position id-layer id)]
                (into [(deleted? edge)]
                      (if incoming?
                        [pos i]
                        [i pos]))))]
      (->> (for [[i edges] (indexed (reverse edges-seq))
                 [id edge] edges]
             (let [head-id (or (merge-head id-layer id) id)]
               [(edge-sort-order i id edge)
                [head-id edge]]))
           (sort-by first #(compare %2 %1))
           (map second)
           (reduce (fn [edges [id edge]]
                     (if (and (deleted? edge)
                              (get edges id))
                       edges
                       (adjoin edges {id edge})))
                   {})))))

(defn merge-nodes [id-layer keyseq nodes]
  (if-let [ks (edges-keyseq keyseq)]
    (let [edges (merge-edges id-layer
                             keyseq
                             (map #(get-in % ks) nodes))]
      (-> (reduce adjoin nil
                  (map #(dissoc-in* % ks) nodes))
          (given (seq edges)
                 (assoc-in* ks edges))))
    (reduce adjoin nil nodes)))

(defn- deleted-edge-keyseq [keyseq]
  (if (meta-keyseq? keyseq)
    (match keyseq
      [_ :incoming] [])
    (match keyseq
      [_ :edges _]          [:deleted]
      [_ :edges _ :deleted] [])))

(defn- deleted-node-keyseq [keyseq]
  (when-not (meta-keyseq? keyseq)
    (match keyseq
      [_]          [:deleted]
      [_ :deleted] [])))

(letfn [(exists?  [id-layer id exists]  (and exists (not (node-deleted? id-layer id))))
        (deleted? [id-layer id deleted] (or deleted (node-deleted? id-layer id) deleted))]

  (defn mark-edges-deleted [id-layer keyseq node]
    (if-let [ks (edges-keyseq keyseq)]
      (update-in* node ks fixing map? map-vals-with-keys
                  (if (meta-keyseq? keyseq)
                    (partial exists? id-layer)
                    (fn [id edge]
                      (update edge :deleted (partial deleted? id-layer id)))))
      (if-let [ks (deleted-edge-keyseq keyseq)]
        (update-in* node ks
                    (if (meta-keyseq? keyseq)
                      (partial exists?  id-layer (second keyseq))
                      (partial deleted? id-layer (first  keyseq))))
        node)))

  (defn mark-deleted [id-layer keyseq node]
    (->> (if-let [ks (deleted-node-keyseq keyseq)]
           (update-in* node ks (partial deleted? id-layer (first keyseq)))
           node)
         (mark-edges-deleted id-layer keyseq))))

(defn- expand-keyseq-merges
  [id-layer keyseq]
  (let [edge-key (if (meta-keyseq? keyseq) :incoming :edges)
        [from-id attr to-id & tail] keyseq
        from-ids (reverse (merge-ids id-layer from-id))]
    (if (and to-id (= edge-key attr))
      (for [from-id from-ids
            to-id   (reverse (merge-ids id-layer to-id))]
        `(~from-id edge-key ~to-id ~@tail))
      (for [from-id from-ids]
        (cons from-id (rest keyseq))))))

(defn- merge-fn [id-layer keyseq f]
  (when (seq keyseq)
    (condp contains? f
      #{seq  subseq}  (partial apply merge-sorted <)
      #{rseq rsubseq} (partial apply merge-sorted >)
      (when (= identity f)
        (partial merge-nodes id-layer keyseq)))))

(def ^{:dynamic true} *default-id-layer-name* :id)

(defn merge-head
  "Returns the head-id that a specific node is merged into."
  ([id]
     (merge-head *default-id-layer-name* id))
  ([id-layer id]
     (if (meta-id? id)
       (meta-id (merge-head id-layer (base-id id)))
       (let [id-layer (fix id-layer keyword? layer)]
         (:head (graph/get-node id-layer id))))))

(defn merged-into
  "Returns the list of node ids that are merged into a specific node."
  ([id]
     (merged-into *default-id-layer-name* id))
  ([id-layer id]
     (if (meta-id? id)
       (map meta-id (merged-into id-layer (base-id id)))
       (let [id-layer (fix id-layer keyword? layer)]
         (graph/get-incoming id-layer id)))))

(defn node-deleted?
  "Returns true if the specified node has been deleted."
  ([id]
     (node-deleted? *default-id-layer-name* id))
  ([id-layer id]
     (let [id-layer (fix id-layer keyword? layer)]
       (:deleted (graph/get-node id-layer id)))))

(defn merge-ids
  "Returns a list of node ids in the merge chain of id. Always starts with the head-id,
   followed by tail-ids in the reverse order they were merged."
  ([id]
     (merge-ids *default-id-layer-name* id))
  ([id-layer id]
     (let [id-layer (fix id-layer keyword? layer)]
       (if-let [head-id (merge-head id-layer id)]
         `[~head-id ~@(merged-into id-layer head-id)]
         [id]))))

(defn merge-position
  "Returns the position in the merge chain for a given id, 0 for the head."
  ([id]
     (merge-position *default-id-layer-name* id))
  ([id-layer id]
     (let [id-layer (fix id-layer keyword? layer)
           node     (graph/get-node id-layer id)]
       (when-let [head (:head node)]
         (if (= head id)
           0
           (get-in node [:edges head :position]))))))

(defn merge-node
  "Functional version of merge-node!"
  [id-layer head-id tail-id]
  (verify (not= head-id tail-id)
          (format "cannot merge %s into itself" tail-id))
  (verify (= (type-key head-id) (type-key tail-id))
          (format "cannot merge %s into %s because they are not the same type" tail-id head-id))
  (let [head        (graph/get-node id-layer head-id)
        merge-count (or (:merge-count head) 0)
        head-merged (fix (:head head)               #{head-id} nil)
        tail-merged (fix (merge-head id-layer tail-id) #{tail-id} nil)]
    (if (= head-id tail-merged)
      (printf "warning: %s is already merged into %s\n" tail-id head-id)
      (do
        (verify (not head-merged)
                (format "cannot merge %s into %s because %2$s is already merged into %s"
                        tail-id head-id head-merged))
        (verify (not tail-merged)
                (format "cannot merge %s into %s because %1$s is already merged into %s"
                        tail-id head-id tail-merged))
        (let [revision (current-revision id-layer)
              tail-ids (cons tail-id (merged-into id-layer tail-id))]
          (reduce (fn [layer [pos id]]
                    (graph/update-node layer id adjoin
                                       {:head head-id
                                        :edges {head-id {:revision revision
                                                   :position (+ 1 pos merge-count)}}}))
                  (graph/update-node id-layer head-id adjoin
                                     {:head head-id
                                      :merge-count (+ merge-count (count tail-ids))})
                  (indexed tail-ids)))))))

(defn merge-node!
  "Merge tail node into head node, merging all nodes that are currently merged into tail as well."
  ([head-id tail-id]
     (merge-node! *default-id-layer-name* head-id tail-id))
  ([id-layer head-id tail-id]
     (let [id-layer (fix id-layer keyword? layer)]
       (dotxn id-layer
         (merge-node id-layer head-id tail-id)))))

(defn- delete-merges-after
  "Delete all merges from node that happened at or after revision."
  [id-layer revision id node]
  (graph/update-node id-layer id adjoin
                     {:edges
                      (into {} (for [[to-id edge] (:edges node)
                                     :when (and (not (:deleted edge))
                                                (<= revision (:revision edge)))]
                                 [to-id {:deleted true}]))}))

(defn unmerge-node
  "Functional version of unmerge-node!"
  [id-layer head-id tail-id]
  (let [tail      (graph/get-node id-layer tail-id)
        merge-rev (get-in tail [:edges head-id :revision])
        tail-ids  (merged-into id-layer tail-id)
        head-ids  (merged-into id-layer head-id)]
    (verify merge-rev ;; revision of the original merge of tail into head
            (format "cannot unmerge %s from %s because they aren't merged" tail-id head-id))
    (reduce (fn [layer id]
              (-> layer
                  (delete-merges-after merge-rev id (graph/get-node id-layer id))
                  (graph/update-node id adjoin {:head tail-id})))
            (-> id-layer
                (delete-merges-after merge-rev tail-id tail)
                (graph/update-node tail-id adjoin {:head nil})
                (given (= (count head-ids) (inc (count tail-ids)))
                       (graph/update-node head-id adjoin {:head nil})))
            tail-ids)))

(defn unmerge-node!
  "Unmerge tail node from head node, taking all nodes that were previously merged into tail with it."
  ([head-id tail-id]
     (unmerge-node! *default-id-layer-name* head-id tail-id))
  ([id-layer head-id tail-id]
     (let [id-layer (fix id-layer keyword? layer)]
       (dotxn id-layer
         (unmerge-node id-layer head-id tail-id)))))

(defn delete-node
  "Functional version of delete-node!"
  [id-layer id]
  (graph/update-node id-layer id adjoin {:deleted true}))

(defn delete-node!
  "Mark the specified node as deleted."
  ([id]
     (delete-node! *default-id-layer-name* id))
  ([id-layer id]
     (let [id-layer (fix id-layer keyword? layer)]
       (dotxn id-layer
         (delete-node id-layer id)))))

(defn undelete-node
  "Functional version of undelete-node!"
  [id-layer id]
  (graph/update-node id-layer id adjoin {:deleted false}))

(defn undelete-node!
  "Mark the specified node as not deleted."
  ([id]
     (undelete-node! *default-id-layer-name* id))
  ([id-layer id]
     (let [id-layer (fix id-layer keyword? layer)]
       (dotxn id-layer
         (undelete-node id-layer id)))))

(def ^{:private true} sentinel (Object.))

(defrecord MergeableLayer [layer id-layer]
  Object
  (toString [this] (pr-str this))

  Enumerate
  (node-id-seq [this] (node-id-seq layer))
  (node-seq    [this] (node-seq layer))

  Basic
  (get-node [this id not-found]
    (let [nodes (->> (reverse (merge-ids id-layer id))
                     (map #(get-node layer % sentinel))
                     (remove #(identical? % sentinel)))]
        (if (empty? nodes)
          not-found
          (->> nodes
               (merge-nodes  id-layer [id])
               (mark-deleted id-layer [id])))))

  (assoc-node!  [this id attrs] (assoc-node! layer id attrs))
  (dissoc-node! [this id]       (dissoc-node! layer id))

  Optimized
  (query-fn [this keyseq not-found f]
    (if-let [merge-data (merge-fn id-layer keyseq f)]
      (let [keyseqs (expand-keyseq-merges id-layer keyseq)]
        (fn [& args]
          (->> (merge-data (for [keyseq keyseqs]
                             (apply graph/query-in-node* layer keyseq not-found f args)))
               (mark-deleted id-layer keyseq))))
      (let [query (query-fn this keyseq not-found identity)]
        (fn [& args]
          (apply f (query) args)))))

  (update-fn [this keyseq f] (update-fn layer keyseq f))

  Layer
  (open      [this] (open  layer))
  (close     [this] (close layer))
  (sync!     [this] (sync! layer))
  (optimize! [this] (optimize! layer))
  (truncate! [this] (truncate! layer))

  Schema
  (schema      [this id]       (schema layer id))
  (verify-node [this id attrs] (verify-node layer id attrs))

  ChangeLog
  (get-revisions   [this id]  (get-revisions layer id))
  (get-changed-ids [this rev] (get-changed-ids layer rev))

  WrappedTransactional
  (txn-wrap [this f]
    (let [wrapped (txn-wrap layer ; let layer wrap transaction, but call f with this
                            (fn [_]
                              (f this)))]
      (fn [^MergeableLayer layer]
        (wrapped (.layer layer)))))

  Revisioned
  (at-revision      [this rev] (assoc-record this :layer (at-revision layer rev)))
  (current-revision [this]     (current-revision layer))

  OrderedRevisions
  (max-revision [this] (max-revision layer))

  Preferences
  (manage-changelog? [this] (manage-changelog? layer))
  (manage-incoming?  [this] (manage-incoming?  layer))
  (single-edge?      [this] (single-edge?      layer)))

(defn mergeable-layer [layer id-layer]
  (MergeableLayer. layer id-layer))