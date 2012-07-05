(ns jiraph.merge
  (:use [jiraph.layer :only [Basic Optimized query-fn get-node]]
        [jiraph.core :only [layer]]
        [jiraph.utils :only [meta-keyseq? meta-id? meta-id base-id edges-keyseq]]
        [jiraph.wrapped-layer :only [NodeFilter defwrapped]]
        [useful.map :only [dissoc-in* assoc-in* update-in*]]
        [useful.seq :only [merge-sorted indexed]]
        [useful.fn :only [fix given]]
        [useful.utils :only [adjoin verify]]
        [useful.datatypes :only [assoc-record]]
        [ego.core :only [type-key]])
  (:require [jiraph.graph :as graph]
            [retro.core :as retro]))

(declare merge-ids merge-head merge-position)

(def ^{:dynamic true} *default-merge-layer-name* :id)

(defn- merge-edges [merge-layer keyseq edges-seq]
  (let [incoming? (meta-keyseq? keyseq)
        deleted?  (if incoming? not :deleted)]
    (letfn [(edge-sort-order [i id edge]
              (let [pos (merge-position merge-layer id)]
                (into [(deleted? edge)]
                      (if incoming?
                        [pos i]
                        [i pos]))))]
      (->> (for [[i edges] (indexed (reverse edges-seq))
                 [id edge] edges]
             (let [head-id (or (merge-head merge-layer id) id)]
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

(defn merge-nodes [merge-layer keyseq nodes]
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
  [merge-layer keyseq]
  (let [edge-key (if (meta-keyseq? keyseq) :incoming :edges)
        [from-id attr to-id & tail] keyseq
        from-ids (reverse (merge-ids merge-layer from-id))]
    (if (and to-id (= edge-key attr))
      (for [from-id from-ids
            to-id   (reverse (merge-ids merge-layer to-id))]
        `(~from-id edge-key ~to-id ~@tail))
      (for [from-id from-ids]
        (cons from-id (rest keyseq))))))

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
     (if (meta-id? id)
       (meta-id (merge-head merge-layer (base-id id)))
       (let [merge-layer (fix merge-layer keyword? layer)]
         (:head (graph/get-node merge-layer id))))))

(defn merged-into
  "Returns the list of node ids that are merged into a specific node."
  ([id]
     (merged-into *default-merge-layer-name* id))
  ([merge-layer id]
     (if (meta-id? id)
       (map meta-id (merged-into merge-layer (base-id id)))
       (let [merge-layer (fix merge-layer keyword? layer)]
         (graph/get-incoming merge-layer id)))))

(defn merge-ids
  "Returns a list of node ids in the merge chain of id. Always starts with the head-id,
   followed by tail-ids in the reverse order they were merged."
  ([id]
     (merge-ids *default-merge-layer-name* id))
  ([merge-layer id]
     (let [merge-layer (fix merge-layer keyword? layer)]
       (if-let [head-id (merge-head merge-layer id)]
         `[~head-id ~@(merged-into merge-layer head-id)]
         [id]))))

(defn merge-position
  "Returns the position in the merge chain for a given id, 0 for the head."
  ([id]
     (merge-position *default-merge-layer-name* id))
  ([merge-layer id]
     (let [merge-layer (fix merge-layer keyword? layer)
           node     (graph/get-node merge-layer id)]
       (when-let [head (:head node)]
         (if (= head id)
           0
           (get-in node [:edges head :position]))))))

(defn merge-node
  "Functional version of merge-node!"
  [merge-layer head-id tail-id]
  (verify (not= head-id tail-id)
          (format "cannot merge %s into itself" tail-id))
  (verify (= (type-key head-id) (type-key tail-id))
          (format "cannot merge %s into %s because they are not the same type" tail-id head-id))
  (let [head        (graph/get-node merge-layer head-id)
        merge-count (or (:merge-count head) 0)
        head-merged (fix (:head head)               #{head-id} nil)
        tail-merged (fix (merge-head merge-layer tail-id) #{tail-id} nil)]
    (if (= head-id tail-merged)
      (printf "warning: %s is already merged into %s\n" tail-id head-id)
      (do
        (verify (not head-merged)
                (format "cannot merge %s into %s because %2$s is already merged into %s"
                        tail-id head-id head-merged))
        (verify (not tail-merged)
                (format "cannot merge %s into %s because %1$s is already merged into %s"
                        tail-id head-id tail-merged))
        (let [revision (retro/current-revision merge-layer)
              tail-ids (cons tail-id (merged-into merge-layer tail-id))]
          (reduce (fn [layer [pos id]]
                    (graph/update-node layer id adjoin
                                       {:head head-id
                                        :edges {head-id {:revision revision
                                                   :position (+ 1 pos merge-count)}}}))
                  (graph/update-node merge-layer head-id adjoin
                                     {:head head-id
                                      :merge-count (+ merge-count (count tail-ids))})
                  (indexed tail-ids)))))))

(defn merge-node!
  "Merge tail node into head node, merging all nodes that are currently merged into tail as well."
  ([head-id tail-id]
     (merge-node! *default-merge-layer-name* head-id tail-id))
  ([merge-layer head-id tail-id]
     (let [merge-layer (fix merge-layer keyword? layer)]
       (retro/dotxn merge-layer
         (merge-node merge-layer head-id tail-id)))))

(defn- delete-merges-after
  "Delete all merges from node that happened at or after revision."
  [merge-layer revision id node]
  (graph/update-node merge-layer id adjoin
                     {:edges
                      (into {} (for [[to-id edge] (:edges node)
                                     :when (and (not (:deleted edge))
                                                (<= revision (:revision edge)))]
                                 [to-id {:deleted true}]))}))

(defn unmerge-node
  "Functional version of unmerge-node!"
  [merge-layer head-id tail-id]
  (let [tail      (graph/get-node merge-layer tail-id)
        merge-rev (get-in tail [:edges head-id :revision])
        tail-ids  (merged-into merge-layer tail-id)
        head-ids  (merged-into merge-layer head-id)]
    (verify merge-rev ;; revision of the original merge of tail into head
            (format "cannot unmerge %s from %s because they aren't merged" tail-id head-id))
    (reduce (fn [layer id]
              (-> layer
                  (delete-merges-after merge-rev id (graph/get-node merge-layer id))
                  (graph/update-node id adjoin {:head tail-id})))
            (-> merge-layer
                (delete-merges-after merge-rev tail-id tail)
                (graph/update-node tail-id adjoin {:head nil})
                (given (= (count head-ids) (inc (count tail-ids)))
                       (graph/update-node head-id adjoin {:head nil})))
            tail-ids)))

(defn unmerge-node!
  "Unmerge tail node from head node, taking all nodes that were previously merged into tail with it."
  ([head-id tail-id]
     (unmerge-node! *default-merge-layer-name* head-id tail-id))
  ([merge-layer head-id tail-id]
     (let [merge-layer (fix merge-layer keyword? layer)]
       (retro/dotxn merge-layer
         (unmerge-node merge-layer head-id tail-id)))))

(def ^{:private true} sentinel (Object.))

(defwrapped MergeableLayer [layer merge-layer]
  Basic
  (get-node [this id not-found]
    (let [nodes (->> (reverse (merge-ids merge-layer id))
                     (map #(get-node layer % sentinel))
                     (remove #(identical? % sentinel)))]
        (if (empty? nodes)
          not-found
          (merge-nodes merge-layer [id] nodes))))

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

  NodeFilter
  (keep-node? [this id]
    (= id (merge-head merge-layer id))))

(defn mergeable-layer [layer merge-layer]
  (MergeableLayer. layer merge-layer))