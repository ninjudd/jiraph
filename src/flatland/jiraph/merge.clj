(ns flatland.jiraph.merge
  (:refer-clojure :exclude [merge])
  (:require [clojure.core :as clojure]
            [flatland.jiraph.graph :refer [compose]]
            [flatland.jiraph.ruminate :as ruminate]
            [flatland.jiraph.layer :as layer]
            [flatland.useful.map :refer [update map-vals]]
            [flatland.useful.utils :refer [adjoin invoke verify]]))

(defn merge [head tail-id]
  (update-in head [:edges tail-id] adjoin {:exists true}))

(defn unmerge [head tail-id]
  (update-in head [:edges tail-id] adjoin {:exists false}))

(defn merge-head [read merge-layer node-id]
  ;; ...
  )

(defn- ruminate-merge [merge-layer layers keyseq f args]
  (fn [read]
    (verify (and (#{merge unmerge} f)
                 (= 1 (count keyseq) (count args)))
            "Merge layer only supports functions merge and unmerge, only at the top level.")
    (let [[head-id] keyseq
          [tail-id] args]
      #_(apply compose (for [layer layers])
               (condp = f
                 merge
                 unmerge ...)))))

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
