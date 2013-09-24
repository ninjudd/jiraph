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

;; note layer will need a key codec that supports revision markers somewhere.  can we wrap the key
;; codec of the underlying layer to prepend an int64? probably, but then we are tied to masai.
;; probably better to do that in the client, since it's responsible for picking key codecs.
(defwrapped ResettableLayer [layer revisioning-layer reset? add-revision-to-id]
  layer/Basic
  (get-node [this id not-found]
    (if-let [ids (ids-for revisioning-layer id)]
      (layer/get-node layer (current-id (current-revision this) ids) not-found)
      not-found))

  (update-in-node [this keyseq f args]
    (let [[id keyseq* f* args*] (dispatch-update keyseq f args
                                                 (fn assoc* [id val] [id nil (constantly val) nil])
                                                 (fn dissoc* [id] [id nil dissoc nil])
                                                 (fn update* [id keys] [id keys f args]))
          revisioned-id (first (ids-for revisioning-layer id))]
      (-> (if (reset? keyseq f)
            ;; TODO simple-ioval needs a write function; where on earth will we find one?
            ;; it seems like simple-ioval was designed only for the "bottom-most" layers in the
            ;; hierarchy, since only they know how to write, and we may need to figure out how to
            ;; compose calls to update-in-node instead. the problem with that is, where do we find
            ;; the current revision? only write functions are given layer', and we don't have one.
            ;;
            ;; thought: did we make this impossible on purpose? it seems like we're attempting to
            ;; break the rule that you can't read what's on other layers in order to decide what to
            ;; write to your own; that rule was put in place to ensure that maintaining consistency
            ;; in case of a crash is possible, right?
            (fn [read]
              (let [update-revisions (simple-ioval revisioning-layer
                                                   [id :ids] conj (fn [layer']
                                                                    [(current-revision layer')]))
                    update-layer (simple-ioval layer [] ({dissoc dissoc} f* assoc)
                                               (fn args-to-update [layer']
                                                 (cons
                                                  (add-revision-to-id id (current-revision layer'))
                                                  (when-not (= dissoc f*)
                                                    (apply update-in* (read layer' [revisioned-id])
                                                           keyseq* f* args*)))))]
                (graph/compose update-revisions update-layer)))
            (layer/update-in-node layer (cons revisioned-id keyseq*) f* args*))
          (update-wrap-read (graph/read-wrapper this keyseq f args))))))
