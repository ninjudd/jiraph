(ns flatland.jiraph.merge
  (:use [flatland.jiraph.layer :only [Layer Basic Optimized Parent
                             children child query-fn get-node update-in-node]]
        [flatland.jiraph.core :only [layer unsafe-txn]]
        [flatland.jiraph.utils :only [edges-keyseq keyseq-edge-id]]
        [flatland.jiraph.wrapped-layer :only [NodeFilter defwrapped fix-read update-wrap-read sublayer-matcher]]
        [flatland.useful.map :only [dissoc-in* assoc-in* update-in* filter-vals update]]
        [flatland.useful.seq :only [merge-sorted indexed]]
        [flatland.useful.fn :only [fix given]]
        [flatland.useful.utils :only [adjoin verify invoke map-entry]]
        [flatland.useful.datatypes :only [assoc-record]]
        [flatland.ordered.set :only [ordered-set]]
        [flatland.ego.core :only [type-key]])
  (:require [flatland.jiraph.graph :as graph :refer [compose same?]]
            [flatland.jiraph.ruminate :as ruminate]
            [flatland.retro.core :as retro]))

(declare merge-ids merge-head merge-position)

(def ^:private sentinel (Object.))

(def ^:dynamic *default-merge-layer-name* :id)

(defn- deleted? [edge]
  (not (:exists edge)))

(defn root-edge-finder [read layer]
  (let [{:keys [merge-layer]} layer]
    (memoize
     (fn [id]
       (when-let [merge-edges (read merge-layer [id :edges])]
         (if (next merge-edges)
           (throw (IllegalStateException.
                   (format "Can't read %s, which appears to be a phantom, as it has edges to %s"
                           (pr-str id) (pr-str (keys merge-edges)))))
           (first merge-edges)))))))

(defn leaf-finder [read layer]
  (let [incoming (-> (:merge-layer layer)
                     (child :incoming))]
    (memoize
     (fn [root-id]
       (read incoming [root-id :edges])))))

(defn head-finder [read layer]
  (let [{:keys [merge-layer]} layer]
    (memoize
     (fn [root-id]
       (read merge-layer [root-id :head])))))

(defn M
  "Merge two nodes, to the extent possible at write time. The result of this function will be
  written to the merge-cache."
  [head tail]
  (letfn [(remove-deleted-edges [node]
            (update node :edges filter-vals :exists))]
    (apply adjoin (map remove-deleted-edges [tail head]))))

(defn E
  [edges find-root-edge root->head]
  (->> edges
       (map (fn [[to-id edge]]
              (if-let [[root-id {:keys [position]}] (find-root-edge to-id)]
                [position (root->head root-id) edge]
                [0 to-id edge])))
       (sort-by (comp - first))
       (reduce (fn [edges [position head-id edge]]
                 (update edges head-id adjoin edge))
               {})))

(defn R*
  [read layer keyseq not-found]
  (if-let [chunks (seq (remove #{sentinel}
                               (map #(read (% layer) keyseq sentinel)
                                    [:cache-layer :layer])))]
    (reduce adjoin chunks)
    not-found))

(defn R
  [read layer find-root-edge [node-id & keys :as keyseq] not-found]
  (if-let [[root-id] (find-root-edge node-id)]
    (R* read layer (cons root-id keys) not-found)
    (read layer keyseq not-found)))

(defn- edge-exists? [e]
  (and (not= sentinel e) (:exists e)))

(defn read-one-edge [read layer from-id to-id]
  (let [find-root-edge (root-edge-finder read layer)]
    (if-let [[to-root] (find-root-edge to-id)]
      (let [to-ids (keys (sort-by (comp - :position val)
                                  ((leaf-finder read layer) to-root)))]
        (reduce (fn [[id m] to-id]
                  (let [edge (R read layer find-root-edge [from-id :edges to-id] sentinel)]
                    (when (edge-exists? edge)
                      (map-entry to-id (adjoin m edge)))))
                nil, to-ids))
      (let [edge (R read layer [from-id :edges to-id] sentinel)]
        (when (edge-exists? edge)
          (map-entry to-id edge))))))

(defn read-node
  ([read layer keyseq]
     (read-node read layer keyseq nil))
  ([read layer [id & ks :as keyseq] not-found]
     (if-let [to-id (keyseq-edge-id keyseq)]
       (if-let [edge (read-one-edge read layer id to-id)]
         (get-in (val edge) (drop 2 ks) ;; removing the :edges <id> part, because id might be a tail
                 not-found)
         not-found)
       (let [find-root-edge (root-edge-finder read layer)
             node (R read layer find-root-edge keyseq sentinel)]
         (if (= node sentinel)
           not-found
           (-> (assoc-in* {} ks node)
               (update :edges E find-root-edge (head-finder read layer))
               (get-in ks)))))))

(defn- merge-edges [merge-layer keyseq edges-seq]
  (letfn [(edge-sort-order [i id edge]
            [(deleted? edge) i (merge-position merge-layer id)])]
    (->> (for [[i edges] (indexed (reverse edges-seq))
               [id edge] edges]
           (let [head-id (or (merge-head merge-layer id) id)]
             [(edge-sort-order i id edge)
              [head-id edge]]))
         (sort-by first #(compare %2 %1))
         (map second)
         (reduce (fn [edges [id edge]]
                   (if (deleted? (get edges id))
                     (assoc edges id edge)
                     (adjoin edges {id edge})))
                 {}))))

(defn- merge-nodes [merge-layer keyseq nodes]
  (if-let [ks (edges-keyseq keyseq)]
    (let [edges (merge-edges merge-layer
                             keyseq
                             (map #(get-in % ks) nodes))]
      (-> (reduce adjoin nil
                  (map #(dissoc-in* % ks) nodes))
          (given (seq edges)
                 (assoc-in* ks edges))))
    (reduce adjoin nil nodes)))

(defn- expand-keyseq-merges
  ([merge-layer keyseq]
     (expand-keyseq-merges graph/get-in-node merge-layer keyseq))
  ([read merge-layer keyseq]
     (let [[from-id attr to-id & tail] keyseq
           from-ids (reverse (merge-ids read merge-layer from-id))]
       (if (and to-id (= :edges attr))
         (for [from-id from-ids
               to-id   (reverse (merge-ids read merge-layer to-id))]
           `(~from-id :edges ~to-id ~@tail))
         (for [from-id from-ids]
           (cons from-id (rest keyseq)))))))

(defn- merge-fn [merge-layer keyseq f]
  (when (seq keyseq)
    (condp contains? f
      #{seq  subseq}  (partial apply merge-sorted <)
      #{rseq rsubseq} (partial apply merge-sorted >)
      (when (= identity f)
        (partial merge-nodes merge-layer keyseq)))))

(defn merge-head
  "Returns the head-id that a specific node is merged into."
  ([id]
     (merge-head *default-merge-layer-name* id))
  ([merge-layer id]
     (merge-head graph/get-in-node merge-layer id))
  ([read merge-layer id]
     (let [merge-layer (fix merge-layer keyword? layer)]
       (read merge-layer [id :head]))))

(defn merged-into
  "Returns a vector of the node ids that are merged into a specific node."
  ([id]
     (merged-into *default-merge-layer-name* id))
  ([merge-layer id]
     (merged-into graph/get-in-node merge-layer id))
  ([read merge-layer id]
     (let [merge-layer (fix merge-layer keyword? layer)]
       (->> (read (child merge-layer :incoming) [id :edges])
            (remove (comp deleted? val))
            (sort-by (comp :position val))
            (keys)
            (into (ordered-set))))))

(defn merge-ids
  "Returns a list of node ids in the merge chain of id. Always starts with the head-id,
   followed by tail-ids in the reverse order they were merged."
  ([id]
     (merge-ids *default-merge-layer-name* id))
  ([merge-layer id]
     (merge-ids graph/get-in-node merge-layer id))
  ([read merge-layer id]
     (let [merge-layer (fix merge-layer keyword? layer)]
       (if-let [head-id (merge-head read merge-layer id)]
         `[~head-id ~@(merged-into read merge-layer head-id)]
         [id]))))

(defn merge-position
  "Returns the position in the merge chain for a given id, 0 for the head."
  ([id]
     (merge-position *default-merge-layer-name* id))
  ([merge-layer id]
     (merge-position graph/get-in-node merge-layer id))
  ([read merge-layer id]
     (let [merge-layer (fix merge-layer keyword? layer)
           node (read merge-layer [id])]
       (when-let [head (:head node)]
         (if (= head id)
           0
           (get-in node [:edges head :position]))))))

(declare wrap-merging)

(defn merge-node
  "Merge tail node into head node, merging all nodes that are currently merged into tail as well."
  [merge-layer head-id tail-id]
  (verify (not= head-id tail-id)
          (format "cannot merge %s into itself" tail-id))
  (verify (= (type-key head-id) (type-key tail-id))
          (format "cannot merge %s into %s because they are not the same type" tail-id head-id))
  (fn [read]
    (let [head (read merge-layer [head-id])
          merge-count (or (:merge-count head) 0)
          head-merged (fix (:head head) #{head-id} nil)
          tail-merged (fix (merge-head read merge-layer tail-id) #{tail-id} nil)]
      (if (= head-id tail-merged)
        (do (printf "warning: %s is already merged into %s\n" tail-id head-id) [])
        (do (verify (not head-merged)
                    (format "cannot merge %s into %s because %2$s is already merged into %s"
                            tail-id head-id head-merged))
            (verify (not tail-merged)
                    (format "cannot merge %s into %s because %1$s is already merged into %s"
                            tail-id head-id tail-merged))
            (let [revision (retro/current-revision merge-layer)
                  tail-ids (cons tail-id (merged-into read merge-layer tail-id))]
              (-> (apply compose
                         (graph/update-node merge-layer head-id adjoin
                                            {:head head-id
                                             :merge-count (+ merge-count (count tail-ids))})
                         (for [[pos id] (indexed tail-ids)]
                           (graph/update-node merge-layer id adjoin
                                              {:head head-id
                                               :edges {head-id {:exists true
                                                                :revision revision
                                                                :position (+ 1 pos merge-count)}}})))
                  (wrap-merging merge-layer)
                  (invoke read))))))))

(defn merge-node!
  "Mutable version of merge-node."
  ([head-id tail-id]
     (merge-node! *default-merge-layer-name* head-id tail-id))
  ([merge-layer head-id tail-id]
     (let [merge-layer (fix merge-layer keyword? layer)]
       (unsafe-txn
         (merge-node merge-layer head-id tail-id)))))

(defn- delete-merges-after
  "Delete all merges from node that happened at or after revision."
  [merge-layer revision id node]
  (graph/update-node merge-layer id adjoin
                     {:edges
                      (into {} (for [[to-id edge] (:edges node)
                                     :when (and (:exists edge)
                                                (<= revision (:revision edge)))]
                                 [to-id {:exists false}]))}))

;; TODO update merge-count on unmerge-node
(defn unmerge-node
  "Unmerge tail node from head node, taking all nodes that were previously merged into tail with it."
  [merge-layer head-id tail-id]
  (fn [read]
    (let [tail (read merge-layer [tail-id])
          merge-rev (get-in tail [:edges head-id :revision])
          tail-ids (merged-into read merge-layer tail-id)
          head-ids (merged-into read merge-layer head-id)]
      (verify merge-rev ;; revision of the original merge of tail into head
              (format "cannot unmerge %s from %s because they aren't merged"
                      tail-id head-id))
      (-> (apply compose
                 (delete-merges-after merge-layer merge-rev tail-id tail)
                 (graph/update-node merge-layer tail-id adjoin {:head nil})
                 (when (= (count head-ids) (inc (count tail-ids)))
                   (graph/update-node merge-layer head-id adjoin {:head nil}))
                 (for [id tail-ids]
                   (compose (delete-merges-after merge-layer merge-rev id (read merge-layer [id]))
                            (graph/update-node merge-layer id adjoin {:head tail-id}))))
          (wrap-merging merge-layer)
          (invoke read)))))

(defn unmerge-node!
  "Mutable version of unmerge-node."
  ([head-id tail-id]
     (unmerge-node! *default-merge-layer-name* head-id tail-id))
  ([merge-layer head-id tail-id]
     (let [merge-layer (fix merge-layer keyword? layer)]
       (unsafe-txn
         (unmerge-node merge-layer head-id tail-id)))))

(defwrapped MergeableLayer [layer merge-layer]
  Basic
  (get-node [this id not-found]
    (read-node graph/get-in-node this [id] not-found))

  (update-in-node [this keyseq f args]
    (-> (update-in-node layer keyseq f args)
        (wrap-merging merge-layer)))

  Layer
  (same? [this other]
    (and (graph/same? layer (:layer other))
         (graph/same? merge-layer (:merge-layer other))))

  ;; *** TODO ***
  ;; we can't support seq-fn like seq/rseq (used for pagination) on a merged layer if we do the
  ;; merging at read time. it would probably also break indexes on mergeable layers.
  ;;
  ;; so, we planned to do all merging at write time, by having a "merged" layer; this layer stores
  ;; already-merged data for full nodes, including merging of their edges. however, this would
  ;; require us to store only the most-recent data, meaning we couldn't give historical reads on
  ;; layers with merging
  ;;
  ;; what if a merge layer were given *two* layers to store its caches on? one to store the value at
  ;; the time of the merge, so that we can do fast historical reads, and one to store the most
  ;; up-to-date version of the node, so that we can quickly update the to-ids of edges to nodes that
  ;; become tails in a merge.
  ;;
  ;; what if a merge layer did its merging of edges only at read time? then you can't paginate, but
  ;; you can do historical reads quickly. then you can wrap this in another layer that caches the
  ;; current post-merge status of :edges, and use that for pagination. then historical reads
  ;; delegate to the "lower" merge layer, but don't support pagination except of the most recent
  ;; revision.
  Optimized
  (query-fn [this keyseq not-found f]
    (if-let [merge-data (merge-fn merge-layer keyseq f)]
      (let [keyseqs (expand-keyseq-merges merge-layer keyseq)]
        (fn [& args]
          (let [nodes (->> keyseqs
                           (map #(apply graph/query-in-node* layer % sentinel f args))
                           (remove #(identical? % sentinel)))]
            (if (empty? nodes)
              not-found
              (merge-data nodes)))))
      (let [query (query-fn this keyseq not-found identity)]
        (fn [& args]
          (apply f (query) args)))))

  Parent
  (children [this]
    (cons :merge (children layer)))

  ;; TODO this will likely need a way to configure whether to wrap certain kinds of children
  (child [this name]
    (if (= :merge name)
      merge-layer
      (when-let [child (child layer name)]
        (retro/at-revision (MergeableLayer. child merge-layer)
                           (retro/current-revision this)))))

  NodeFilter
  (keep-node? [this id]
    (= id (merge-head merge-layer id))))

(defn wrap-merging
  "Wrap an ioval so that reads to any layer which has the given merge layer have merging applied."
  [ioval merge-layer]
  (let [merge-layer? (sublayer-matcher MergeableLayer :merge-layer merge-layer)]
    (update-wrap-read ioval fix-read merge-layer? #(partial read-node %))))

(defn make [layer merge-layer]
  (MergeableLayer. layer merge-layer))

(defn merge-layer
  "Merge layers have to have an :incoming child layer and propagate :position to it. This is a
  helper method that takes care of that for you."
  [layer incoming-layer]
  (ruminate/incoming layer incoming-layer
                     (fn [edge]
                       (not-empty (select-keys edge [:exists :position])))))
