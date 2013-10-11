(ns flatland.jiraph.merge
  (:refer-clojure :exclude [merge])
  (:require [clojure.core :as clojure]
            [flatland.jiraph.graph :as graph :refer [compose update-in-node get-in-node assoc-node]]
            [flatland.jiraph.ruminate :as ruminate]
            [flatland.jiraph.debug :refer [?rev]]
            [flatland.jiraph.layer :as layer :refer [child dispatch-update]]
            [flatland.retro.core :as retro :refer [at-revision]]
            [flatland.useful.map :refer [update assoc-in* filter-keys-by-val map-keys map-vals filter-vals]]
            [flatland.useful.seq :refer [assert-length]]
            [flatland.useful.utils :refer [adjoin invoke verify]]))

(defn M
  [head tail]
  (letfn [(remove-deleted-edges [node]
            (update node :edges filter-vals :exists))]
    (apply adjoin (map remove-deleted-edges [tail head]))))

(letfn [(throw-up! []
          (throw (Exception. (str "You're not supposed to call this function; update-in-node on the"
                                  " merge layer treats it specially and doesn't call it."))))]
  (defn merge [head tail-id phantom-id]
    (throw-up!))

  (defn unmerge [head tail-id]
    (throw-up!)))

(defn- verify-merge-args! [keyseq f args]
  (verify (and (#{merge unmerge} f)
               (= 1 (count keyseq))
               (= (count args) (if (= f merge) 2 1)))
          "Merge layer only supports functions merge and unmerge, only at the top level."))

(defn compose-with [read & iovals]
  ((apply compose iovals) read))

(defn existing-edges [read layer id]
  (seq (->> (read layer [id :edges])
            (filter (comp :exists val)))))

(defn root-edge-finder [read merge-layer]
  (memoize
   (fn [id]
     (when-let [merge-edges (existing-edges read merge-layer id)]
       (verify (not (next merge-edges))
               (format "Can't read %s, which appears to be a phantom, as it has edges to %s"
                       (pr-str id) (pr-str (keys merge-edges))))
       (when-first [edge merge-edges]
         (verify (:position (val edge))
                 (format "No position found on edge from %s to root - maybe not a leaf?"
                         (pr-str (key edge))))
         edge)))))

(defn leaf-finder [read merge-layer]
  (let [incoming (child merge-layer :incoming)]
    (memoize
     (fn [root-id]
       (read incoming [root-id :edges])))))

(defn parent-finder [read merge-layer]
  (let [incoming (child merge-layer :incoming)]
    (memoize
     (fn [id]
       (when-let [edges (existing-edges read incoming id)]
         (verify (not (next edges))
                 (format "Node %s has multiple incoming edges %s - maybe a root?"
                         (pr-str id) (pr-str (keys edges))))
         (key (first edges)))))))

(defn child-finder [read merge-layer]
  (memoize
   (fn [id]
     (->> (existing-edges read merge-layer id)
          (remove (comp :position val))
          (keys)))))

(defn leaf-seq [children id]
  (if-let [cs (seq (children id))]
    (mapcat (partial leaf-seq children) cs)
    [id]))

(defn head-finder [read merge-layer]
  (memoize
   (fn [root-id]
     (read merge-layer [root-id :head]))))

(defn root-or-self [get-root id]
  (if-let [[root] (get-root id)]
    root
    id))

(defn edge-merger [read merge-layer]
  (let [get-root (root-edge-finder read merge-layer)
        get-head (head-finder read merge-layer)]
    (fn E [edges]
      (->> edges
           (map (fn [[to-id edge]]
                  (if-let [[root-id {:keys [position]}] (get-root to-id)]
                    [position (get-head root-id) edge]
                    [0 to-id edge])))
           (sort-by (comp - first))
           (reduce (fn [edges [position head-id edge]]
                     (update edges head-id adjoin edge))
                   nil)))))

(defn reassemble-merged-node [read merge-layer base-layer head-id]
  (let [phantom-layer (child base-layer :phantom)
        get-children (child-finder read merge-layer)
        get-root (root-edge-finder read merge-layer)]
    (letfn [(first-merged [id]
              (first (layer/get-revisions merge-layer id)))
            (get-before [leaf-id revision]
              (-> (at-revision base-layer (dec revision))
                  (graph/get-node leaf-id)))
            (merged-node* [id]
              (if-let [children (seq (get-children id))]
                (-> (reduce M (map merged-node* children))
                    (adjoin (read phantom-layer [id])))
                (let [merge-revision (first-merged id)]
                  (get-before id merge-revision))))]
      (merged-node* (root-or-self get-root head-id)))))

(defn merger [merge-layer layers keyseq f args & {merge-fn :merge unmerge-fn :unmerge}]
  (verify (and merge-fn unmerge-fn) "Gotta pass em all")
  (verify-merge-args! keyseq f args)
  (fn [read]
    (let [[head-id] keyseq
          [tail-id] args
          impl ({merge merge-fn, unmerge unmerge-fn} f)]
      (compose-with read
        (apply update-in-node merge-layer keyseq f args)
        (for [layer layers]
          (fn [read]
            (compose-with read (impl head-id tail-id layer read))))))))

(defn- ruminate-merge-nodes [merge-layer layers keyseq f args]
  (merger merge-layer layers keyseq f args
    :merge (fn [head-id tail-id layer read]
             ;; write merged head and tail to the head, and delete the tail
             [(update-in-node layer [] dissoc tail-id)
              (update-in-node layer [] assoc
                              head-id (M (read layer [head-id])
                                         (read layer [tail-id])))])
    :unmerge (fn [head-id tail-id layer read]
               ;; re-compute merged view of head and tail, with new merge history/tree
               (for [id [head-id tail-id]]
                 (update-in-node layer [] assoc id
                                 (reassemble-merged-node read merge-layer layer id))))))

(defn ruminate-merge-edges [merge-layer layers keyseq f args]
  (merger merge-layer layers keyseq f args
    :merge (fn [head-id tail-id layer read]
             [(when-let [merged-nodes (child layer :without-edge-merging)]
                ;; merge edges on the head, and delete the tail
                [(update-in-node layer [] dissoc tail-id)
                 (update-in-node layer [] assoc
                                 head-id (update (read merged-nodes [head-id])
                                                 :edges (edge-merger read merge-layer)))])
              (when-let [incoming (child layer :incoming)]
                ;; use incoming layer to find all edges to the tail, and point them at the
                ;; head instead
                (for [[from-id incoming-edge] (read incoming [tail-id :edges])
                      :when (and (:exists incoming-edge)
                                 (distinct? from-id head-id tail-id))]
                  ;; combine the edges to the head and tail together, letting head win and
                  ;; ignoring deleted edges
                  (let [new-edge (reduce adjoin
                                         (->> (for [to-id [tail-id head-id]]
                                                (read layer [from-id :edges to-id]))
                                              (filter :exists)))]
                    (update-in-node layer [from-id :edges] adjoin
                                    {tail-id {:exists false} ;; delete the edge to the tail
                                     head-id new-edge}))))]) ;; and write it to the head
    :unmerge (fn [head-id tail-id layer read]
               (let [E (edge-merger read merge-layer)
                     merged-nodes (child layer :without-edge-merging)
                     read-layer (or merged-nodes layer)]
                 ;; re-merge edges on head-id, tail-id, and every node with an edge to the head-id.
                 (for [id (distinct (concat (when merged-nodes
                                              [head-id tail-id])
                                            (when-let [incoming (child layer :incoming)]
                                              (->> (read incoming [head-id :edges])
                                                   (filter-keys-by-val :exists)))))]
                   (update-in-node layer [] assoc
                                   id (update (read read-layer [id]) :edges E)))))))

;; options:
;;
;; a) commit to this big chronology tree, and live with the fact that, when unmerging node X, we
;; have to reconstruct the history of all nodes that have edges to X, to ensure that their edges
;; point to where they did before the merge happened.
;;
;; b) store a copy of every node without edge merging applied, so that when X is unmerged we can
;; just take the stored copy of each node with an edge to X, apply E to it, and write that.  - one
;; detail with this is that whenever a node is merged, we have to apply E to it again, so that
;; merges can always happen in a consistent reproducible order (from-id before to-id). otherwise, it
;; will be impossible for an unmerge to reconstruct the merge history in a consistent way.

(defn- update-leaves [layer new-root leaves-with-old-roots]
  (->> leaves-with-old-roots
       (map-indexed (fn [i [leaf-id old-root]]
                      (cons (update-in-node layer [leaf-id :edges new-root]
                                            adjoin {:exists true, :position i})
                            (when old-root
                              [(update-in-node layer [leaf-id :edges old-root]
                                               adjoin {:exists false})]))))))

(defn leaves-with-roots [get-root get-leaves id]
  (if-let [[old-root] (get-root id)]
    (->> (get-leaves old-root)
         (sort-by (comp :position val))
         (map #(list (key %) old-root)))
    [[id]]))

(defn- create-root
  "Create new root at [root-id], adding edges from it to the roots of [head-id] and [tail-id] and
   setting its :head to the first of them."
  [layer get-root root-id head-id tail-id]
  (update-in-node layer [root-id]
                  adjoin {:head head-id
                          :edges (into {}
                                       (for [id [head-id tail-id]]
                                         [(root-or-self get-root id)
                                          {:exists true}]))}))

(defn merge-point [get-parent get-head tail-id]
  (let [parents (rest (iterate get-parent tail-id))
        ;; walk up parent chain until tail-id is no longer the head - this is where
        ;; tail-id first became a tail of the current merge
        child-root (last (cons tail-id
                               (take-while #(= tail-id (get-head %))
                                           parents)))]
    {:child child-root
     :parent (get-parent child-root)}))

(defn ruminate-merge [merge-layer [] keyseq f args]
  (verify-merge-args! keyseq f args)
  (fn [read]
    (let [mread (memoize read)
          [head-id] keyseq
          [tail-id root-id] args
          get-head (head-finder mread merge-layer)
          get-root (root-edge-finder mread merge-layer)
          get-leaves (leaf-finder mread merge-layer)]
      (condp = f
        merge (if (seq (read merge-layer [root-id]))
                (throw (IllegalStateException.
                        (format "Can't use %s as root of new merge, as it already exists"
                                (pr-str root-id))))
                (compose-with read
                  ;; point head's and tail's leaves at new root
                  (update-leaves merge-layer root-id
                                 (mapcat (partial leaves-with-roots get-root get-leaves)
                                         [head-id tail-id]))
                  ;; create new root, above old roots
                  (create-root merge-layer get-root root-id head-id tail-id)))
        unmerge (let [get-parent (parent-finder mread merge-layer)
                      get-children (child-finder mread merge-layer)
                      [root] (get-root tail-id)]
                  (verify root
                          (format "Can't unmerge %s from %s, as it is not merged into anything"
                                  (pr-str tail-id) (pr-str head-id)))
                  (verify (= head-id (get-head root))
                          (format "Can't unmerge %s from %s, as its head is actually %s"
                                  (pr-str tail-id) (pr-str head-id) (pr-str (get-head root))))
                  (let [{:keys [child parent]} (merge-point get-parent get-head tail-id)]
                    (compose-with read
                      ;; disconnect tail's new root from the place where it was merged into head
                      (update-in-node merge-layer [parent :edges child]
                                      adjoin {:exists false})
                      ;; update each of tail's leaves to point at its new root
                      (for [leaf-id (leaf-seq get-children tail-id)]
                        (update-in-node merge-layer [leaf-id :edges] adjoin
                                        (into {root {:exists false}} ;; disconnect from old root
                                              (when (not= child leaf-id)
                                                {child (val (get-root leaf-id))})))))))))))

;; - what revision tail was merged into head
;; - get versions of head/tail just prior to merge
;;

;; what about:
;; - two layers of merging ruminants:
;;   - one that doesn't merge edge destination ids at all, just merging node data (including edges)
;;   - one that ruminates on the above, and does just edge-destination merging
;; - can unmerge by looking at a historical view of the "less-merged" layer above you, and
;;   then re-computing all the merges that aren't being undone

(defn merge-head-finder* [get-root get-head]
  (fn [id]
    (when-let [[root-id] (get-root id)]
      (get-head root-id))))

(defn merge-head-finder [read merge-layer]
  (merge-head-finder* (root-edge-finder read merge-layer)
                      (head-finder read merge-layer)))

(defn merge-head [read merge-layer id]
  ((merge-head-finder read merge-layer) id))

(defn verify-adjoin! [f why]
  (verify (= f adjoin)
          (format "Can't apply non-adjoin function %s%s"
                  (symbol (.getName (class f)))
                  why)))

(defn- ruminate-merging-nodes [layer [merge-layer] keyseq f args]
  (let [phantom (child layer :phantom)]
    (fn [read]
      (compose-with read
        (let [find-root (root-edge-finder read merge-layer)
              get-head (head-finder read merge-layer)
              head+root (fn [id]
                          (if-let [[root] (find-root id)]
                            [(get-head root) root]
                            [id]))
              [layer-args phantom-args] (dispatch-update keyseq f args
                                                         (fn assoc* [id val]
                                                           (for [id (head+root id)]
                                                             [[] assoc id val]))
                                                         (fn dissoc* [id]
                                                           (for [id (head+root id)]
                                                             [[] dissoc id]))
                                                         (fn update* [id keys]
                                                           (for [id (head+root id)]
                                                             (list* (cons id keys) f args))))]
          [(apply update-in-node layer layer-args)
           (when (and phantom phantom-args)
             (verify-adjoin! f " to phantom layer, as it would not be unmergeable")
             (apply update-in-node phantom phantom-args))])))))

(defn- ruminate-merging-edges [layer [node-merging-only merge-layer] keyseq f args]
  (fn [read]
    (compose-with read
      (verify-adjoin! f " because handling it would be hard.")
      (apply update-in-node node-merging-only keyseq f args)
      (let [merge-head (merge-head-finder read merge-layer)
            [from-id & keys] keyseq]
        (update-in-node layer [from-id] adjoin
                        (-> (apply assoc-in* {} keys (assert-length 1 args))
                            (update :edges map-keys #(or (merge-head %) %))))))))

(defn make
  "layers needs to be a map of layer names to base layers. The base layer will be used to store a
   merged view of tha data written to the merging layer, as determined by merges written to the
   merge-layer. Each base layer must have a child named :phantom, which will be used to store
   internal bookkeeping data, and should not be used by client code.

   Will return a list, [new-merge-layer [merging-layer1 merging-layer2 ...]].

   Writes to these returned layers will automatically update each other as needed to keep the merged
   views consistent."
  [merge-layer layers]
  [(-> merge-layer
       (ruminate/make [] ruminate-merge)  ;; M.r
       (ruminate/make (for [layer layers] ;; M.n
                        (child layer :without-edge-merging))
                      ruminate-merge-nodes)
       (ruminate/make layers ruminate-merge-edges)) ;; M.e
   (for [layer layers]
     (let [node-merging-only (ruminate/make (child layer :without-edge-merging) ;; A.n
                                            [merge-layer]
                                            ruminate-merging-nodes)]
       (ruminate/make layer [node-merging-only merge-layer] ;; A.e
                      ruminate-merging-edges)))])


#_(merged m [(parent/make tree-base {:phantom tree-phantom})
             (parent/make data-base {:phantom data-phantom})])
