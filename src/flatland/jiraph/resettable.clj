(ns flatland.jiraph.resettable
  (:use flatland.jiraph.wrapped-layer
        flatland.useful.debug
        [flatland.useful.utils :only [returning adjoin verify invoke map-entry]]
        [flatland.useful.seq :only [assert-length]]
        [flatland.useful.map :only [assoc-in* update-in*]])
  (:require [flatland.jiraph.layer :as layer :refer [dispatch-update]]
            [flatland.jiraph.graph :as graph :refer [update-in-node get-in-node simple-ioval]]
            [flatland.retro.core :as retro :refer [at-revision current-revision]])
  (:use flatland.useful.debug))

(defn- get-edition
  ([revisioning-layer id revision]
     (get-edition graph/get-in-node revisioning-layer id revision))
  ([read revisioning-layer id revision]
     (-> revisioning-layer
         (at-revision revision)
         (read [id :edition]))))

;; note layer will need a key codec that supports revision markers somewhere.  can we wrap the key
;; codec of the underlying layer to prepend an int64? probably, but then we are tied to masai.
;; probably better to do that in the client, since it's responsible for picking key codecs.
(defwrapped ResettableLayer
  [layer revisioning-layer reset? add-edition-to-id]
  [layer [layer revisioning-layer]]
  layer/Basic
  (get-node [this id not-found]
    (let [revision (current-revision this)]
      (if-let [current-edition (get-edition revisioning-layer id revision)]
        (-> layer
            (at-revision revision)
            (layer/get-node (add-edition-to-id id current-edition) not-found))
        not-found)))

  (update-in-node [this keyseq f args]
    (fn [read]
      (let [[id keyseq* f* args*] (dispatch-update keyseq f args
                                                   (fn assoc* [id val] [id nil (constantly val) nil])
                                                   (fn dissoc* [id] [id nil ::dissoc nil])
                                                   (fn update* [id keys] [id keys f args]))
            revision (current-revision this)
            old-edition (or (get-edition read revisioning-layer id revision) 0)
            old-id (add-edition-to-id id old-edition)]
        (-> (if (reset? keyseq f)
              (let [new-id (add-edition-to-id id (inc old-edition))]
                (graph/compose (layer/update-in-node (-> revisioning-layer (at-revision revision))
                                                     [id :edition] adjoin [(inc old-edition)])
                               (when-not (= f* ::dissoc)
                                 (layer/update-in-node (-> layer (at-revision revision))
                                                       [] assoc
                                                       [new-id
                                                        (apply update-in* (read layer [old-id])
                                                               keyseq* f* args*)]))))
              (layer/update-in-node layer (cons old-id keyseq*) f* args*))
            (update-wrap-read (graph/read-wrapper this keyseq f args))
            (invoke read)))))

  layer/Optimized
  (query-fn [this keyseq not-found f]
    (when-let [[id & keys] (seq keyseq)]
      (when-let [edition (get-edition revisioning-layer id (current-revision this))]
        (layer/query-fn layer (cons (add-edition-to-id id edition)
                                    keys)
                        not-found f))))

  layer/ChangeLog
  (get-revisions [this id]
    (seq (into (sorted-set)
               (for [reset-revision (layer/get-revisions revisioning-layer id)
                     :let [edition (get-edition revisioning-layer id reset-revision)]
                     revision (layer/get-revisions layer (add-edition-to-id id edition))]
                 revision))))

  (get-changed-ids [this rev]
    nil)

  layer/EnumerateIds
  (node-id-seq [this opts]
    (layer/node-id-seq revisioning-layer opts))

  layer/Enumerate
  (node-seq [this opts]
    (for [id (layer/node-id-seq this opts)]
      (map-entry id (layer/get-node this id nil))))


  #_(



     Parent
     (children [this#] (children ~layer-sym))
     (child    [this# kind#] (child ~layer-sym kind#))))

(defn make
  [layer revisioning-layer {:keys [reset? add-edition-to-id] :as opts
                            :or {reset? (fn [keyseq f] (not= f adjoin))
                                 add-edition-to-id (fn [id edition]
                                                     (str edition "@" id))}}]
  (ResettableLayer. layer revisioning-layer reset? add-edition-to-id))