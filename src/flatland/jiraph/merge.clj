(ns flatland.jiraph.merge
  (:refer-clojure :exclude [merge])
  (:require [clojure.core :as clojure]
            [flatland.jiraph.graph :refer [compose update-in-node get-in-node assoc-node]]
            [flatland.jiraph.ruminate :as ruminate]
            [flatland.jiraph.layer :as layer :refer [child]]
            [flatland.retro.core :refer [at-revision]]
            [flatland.useful.map :refer [update map-vals filter-vals]]
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

(defn merge-head [read merge-layer node-id]
  ;; ...
  )

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
    (mapcat (partial leaf-seq children) id)
    [id]))

(defn head-finder [read merge-layer]
  (memoize
   (fn [root-id]
     (read merge-layer [root-id :head]))))

(defmacro merge-impl [& {:keys [merge unmerge]}]
  (verify (and merge unmerge) "Gotta pass em all")
  `(fn [read#]
     (let ~'[[head-id] keyseq
             [tail-id] args]
       (compose-with read#
         (apply update-in-node ~'merge-layer ~'keyseq ~'f ~'args
                (for ~'[layer layers]
                  (condp = ~'f
                    merge (fn ~'[read]
                            (compose-with ~'read ~merge))
                    unmerge (fn ~'[read]
                              (compose-with ~'read ~unmerge)))))))))

(defn- ruminate-merge-node [merge-layer layers keyseq f args]
  (merge-impl
   :merge (let [head (read layer [head-id])
                tail (read layer [tail-id])]
            ;; write (M head tail) to the head, and delete the tail
            [(update-in-node layer [] assoc head-id (M head tail))
             (update-in-node layer [] dissoc tail-id)])
   :unmerge '(let [merge-rev '...
                   before-merge (at-revision layer (dec merge-rev))
                   [head tail] (for [id [head-id tail-id]]
                                 (get-in-node before-merge [id]))]
               (assoc-node layer tail-id (E* read tail))
               ;; walk
               )))

(defn ruminate-merge-edges [merge-layer layers keyseq f args]
  (merge-impl
   :merge (when-let [incoming (child layer :incoming)]
            ;; use incoming layer to find all edges to the tail, and point them at the
            ;; head instead
            (for [[from-id incoming-edge] (read incoming [tail-id :edges])
                  :when (:exists incoming-edge)]
              ;; combine the edges to the head and tail together, letting head win and
              ;; ignoring deleted edges
              (let [new-edge (reduce adjoin
                                     (->> (for [to-id [tail-id head-id]]
                                            (read layer [from-id :edges to-id]))
                                          (filter :exists)))]
                (update-in-node layer [from-id :edges] adjoin
                                {tail-id {:exists false} ;; delete the edge to the tail
                                 head-id new-edge})))))  ;; and write it to the head
  :unmerge '...)

(defn- update-leaves [layer new-root leaves-with-old-roots]
  (map-indexed (fn [i [leaf-id old-root]]
                 (cons (update-in-node layer [leaf-id :edges new-root]
                                       adjoin {:exists true, :posiion i})
                       (when old-root
                         [(update-in-node layer [leaf-id :edges old-root]
                                          adjoin {:exists false})])))
               leaves-with-old-roots))

(defn leaves-with-roots [get-root get-leaves id]
  (if-let [[old-root] (get-root id)]
    (->> (get-leaves old-root)
         (sort-by (comp val :position))
         (map #(list (key %) old-root)))
    [[id]]))

(defn root-or-self [get-root id]
  (if-let [[root] (get-root id)]
    root
    id))

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

(defn ruminate-merge [layer [] keyseq f args]
  (verify-merge-args! keyseq f args)
  (fn [read]
    (let [mread (memoize read)
          [head-id] keyseq
          [tail-id root-id] args
          get-head (head-finder mread layer)
          get-root (root-edge-finder mread layer)
          get-leaves (leaf-finder mread layer)]
      (condp = f
        merge (if (seq (read layer [root-id]))
                (throw (IllegalStateException.
                        (format "Can't use %s as root of new merge, as it already exists"
                                (pr-str root-id))))
                (compose-with read
                  ;; point head's and tail's leaves at new root
                  (update-leaves layer root-id
                                 (mapcat (partial leaves-with-roots get-root get-leaves)
                                         [head-id tail-id]))
                  ;; create new root, above old roots
                  (create-root layer get-root root-id head-id tail-id)))
        unmerge (let [get-parent (parent-finder mread layer)
                      get-children (child-finder mread layer)
                      [root] (get-root tail-id)]
                  (verify root
                          (format "Can't unmerge %s from %s, as it is not merged into anything"
                                  (pr-str tail-id) (pr-str head-id)))
                  (verify (= head-id (get-head root))
                          (format "Can't unmerge %s from %s, as its head is actually %s"
                                  (pr-str tail-id) (pr-str head-id) (pr-str (get-head root))))
                  (let [parents (iterate get-parent tail-id)
                        ;; walk up parent chain until tail-id is no longer the head - this is where
                        ;; tail-id first became a tail of the current merge
                        tail-root (last (cons tail-id
                                              (take-while #(= tail-id (get-head %))
                                                          parents)))
                        merge-point (get-parent tail-root)]
                    (compose-with read
                      ;; disconnect tail's new root from the place where it was merged into head
                      (update-in-node layer [merge-point :edges tail-root]
                                      adjoin {:exists false})
                      ;; update each of tail's leaves to point at its new root
                      (for [leaf-id (leaf-seq get-children tail-id)]
                        (update-in-node layer [leaf-id :edges] adjoin
                                        {root {:exists false} ;; also disconnect from old root
                                         tail-root (val (get-root leaf-id))})))))))))

;; - what revision tail was merged into head
;; - get versions of head/tail just prior to merge
;;

;; what about:
;; - two layers of merging ruminants:
;;   - one that doesn't merge edge destination ids at all, just merging node data (including edges)
;;   - one that ruminates on the above, and does just edge-destination merging
;; - can unmerge by looking at a historical view of the "less-merged" layer above you, and
;;   then re-computing all the merges that aren't being undone

(defn- ruminate-merging [layer [merge-layer] keyseq f args]
  (fn [read]
    #_(-> (if-let [head (merge-head read merge-layer (get-id keyseq))]
          (let [keyseq (update keyseq for head-id)]
            (apply compose (for [layer [layer (child layer :phantom)]]
                             (update-in-node layer keyseq f args))))
          (update-in-node layer keyseq f args))
        (invoke read))))

(defn merged
  "layers needs to be a map of layer names to base layers. The base layer will be used to store a
   merged view of tha data written to the merging layer, as determined by merges written to the
   merge-layer. Each base layer must have a child named :phantom, which will be used to store
   internal bookkeeping data, and should not be used by client code.

   Will return a list, [new-merge-layer [merging-layer1 merging-layer2 ...]].

   Writes to these returned layers will automatically update each other as needed to keep the merged
   views consistent."
  [merge-layer layers]
  [(ruminate/make merge-layer layers ruminate-merge)
   (for [layer layers]
     (ruminate/make layer [merge-layer] ruminate-merging))])


#_(merged m [(parent/make tree-base {:phantom tree-phantom})
             (parent/make data-base {:phantom data-phantom})])
