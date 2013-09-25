(ns jiraph.layer.resettable
  (:use flatland.jiraph.wrapped-layer
        flatland.useful.debug
        [flatland.useful.utils :only [returning adjoin verify]]
        [flatland.useful.seq :only [assert-length]]
        [flatland.useful.map :only [assoc-in* update-in*]])
  (:require [flatland.jiraph.layer :as layer :refer [dispatch-update]]
            [flatland.jiraph.graph :as graph :refer [update-in-node get-in-node simple-ioval]]
            [flatland.retro.core :as retro :refer [at-revision current-revision]]))

(defn ids-for
  "Return a seq of [revision id] pairs, in decreasing revision order. Each id returned contains all
  data for the requested node at or after the associated revision. For example, ([29 \"x1\"] [0
  \"x0\"]) means that if you want a revision in the range [0,28], then look under \"x0\"; for
  anything 29 and up, look in \"x1\"."
  [revision-layer id]
  ;; we'll be storing revision lists in this order, overwriting rather than appending when something
  ;; new comes along, so that it's faster to read; after all, resetting writes should be much rarer
  ;; than reads.
  (graph/get-in-node revision-layer [id :ids]))

(defn current-id [revision id-list]
  (ffirst (drop-while #(> (first %) revision)
                      id-list)))

(def sentinel (Object.))

;; note layer will need a key codec that supports revision markers somewhere.  can we wrap the key
;; codec of the underlying layer to prepend an int64? probably, but then we are tied to masai.
;; probably better to do that in the client, since it's responsible for picking key codecs.
(defwrapped ResettableLayer [layer revisioning-layer reset? add-revision-to-id]
  layer/Basic
  (get-node [this id not-found]
    (let [revision (current-revision this)
          current-edition (-> revisioning-layer
                              (at-revision revision)
                              (graph/get-in-node [id :current] sentinel))]
      (if (= sentinel current-edition)
        not-found
        (-> layer
            (at-revision revision)
            (layer/get-node (add-revision-to-id id current-edition) not-found)))))

  (update-in-node [this keyseq f args]
    (let [[id keyseq* f* args*] (dispatch-update keyseq f args
                                                 (fn assoc* [id val] [id nil (constantly val) nil])
                                                 (fn dissoc* [id] [id nil dissoc nil])
                                                 (fn update* [id keys] [id keys f args]))
          revision (current-revision this)
          old-edition (-> revisioning-layer
                          (at-revision revision)
                          (graph/get-in-node [id :current] 0))
          old-id (add-revision-to-id id old-edition)]
      (-> (if (reset? keyseq f)
            (graph/compose (layer/update-in-node (-> revisioning-layer (at-revision revision))
                                                 [id :current] adjoin [(inc old-edition)])
                           (fn [read]
                             ((layer/update-in-node (-> layer (at-revision revision))
                                                    [] ({dissoc dissoc} f* assoc)
                                                    (cons (add-revision-to-id id (inc old-edition))
                                                          (when-not (= dissoc f*)
                                                            (apply update-in* (read layer [old-id])
                                                                   keyseq* f* args*))))
                              read)))
            (layer/update-in-node layer (cons old-id keyseq*) f* args*))
          (update-wrap-read (graph/read-wrapper this keyseq f args)))))

  layer/Optimized
  (query-fn [this keyseq not-found f]
    (when-let [[id & keys] (seq keyseq)]
      (let [revision (current-revision this)
            edition (-> revisioning-layer
                        (at-revision revision)
                        (graph/get-in-node [id :current] sentinel))]
        (when (not= edition sentinel)
          (layer/query-fn layer (cons (add-revision-to-id id edition)
                                      keys)
                          not-found f))))))
