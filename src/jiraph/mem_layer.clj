(ns jiraph.mem-layer
  (:use jiraph.layer
        [retro.core :as retro]))

; The mem implementations should work for refs, atoms and agents

(defn- mem-open             [layer])
(defn- mem-close            [layer])
(defn- mem-sync!            [layer])
(defn- mem-optimize!        [layer])
(defn- mem-node-count       [layer] (count @(:nodes layer)))
(defn- mem-node-ids         [layer] (keys @(:nodes layer)))
(defn- mem-fields           [layer] '())
(defn- mem-get-property     [layer key] (get @(:props layer) key))
(defn- mem-get-node         [layer id] (get @(:nodes layer) id))
(defn- mem-node-exists?     [layer id] (contains? @(:nodes layer) id))
(defn- mem-get-revisions    [layer id] id)
(defn- mem-get-incoming     [layer id] (get @(:incoming layer) id))

(def mem-impl
  {:open           mem-open
   :close          mem-close
   :sync!          mem-sync!
   :optimize!      mem-optimize!
   :node-count     mem-node-count
   :node-ids       mem-node-ids
   :fields         mem-fields
   :get-property   mem-get-property
   :get-node       mem-get-node
   :node-exists?   mem-node-exists?
   :get-revisions  mem-get-revisions
   :get-incoming   mem-get-incoming})

(defn- mem-txn-wrap [layer f] f)

(def mem-txn-impl
  {:txn-wrap mem-txn-wrap})

(defn- mem-get-revision
  [layer]
  (get @(:props layer) :rev))

(def mem-revision-impl
  {:get-revision mem-get-revision})


; A ref based implementation

(defn- ref-truncate!
  [layer]
  (dosync
    (ref-set (:nodes layer) {})
    (ref-set (:incoming layer) {})))

(defn- ref-set-property!
  [layer key val]
  (dosync (alter (:props layer) assoc key val)))

(defn- ref-add-node!
  [layer id attrs]
  (dosync (alter (:nodes layer) assoc id attrs)))

(defn- ref-append-node!
  [layer id attrs]
  (dosync (alter (:nodes layer) update-in [id] merge attrs)))

(defn- ref-update-node!
  [layer id f args]
  (dosync
    (let [old (get @(:nodes layer) id)
          new (apply f old args)]
    (alter (:nodes layer) assoc id new)
      [old new])))

(defn- ref-delete-node!
  [layer id]
  (dosync (alter (:nodes layer) dissoc id)))

(defn- ref-add-incoming!
  [layer id from-id]
  (dosync (alter (:incoming layer) assoc id
                 (conj (get @(:incoming layer) id #{}) from-id))))

(defn- ref-drop-incoming!
  [layer id from-id]
  (dosync (alter (:incoming layer) update-in [id] disj from-id)))

(def ref-impl
  {:truncate!      ref-truncate!
   :set-property!  ref-set-property!
   :add-node!      ref-add-node!
   :append-node!   ref-append-node!
   :update-node!   ref-update-node!
   :delete-node!   ref-delete-node!
   :add-incoming!  ref-add-incoming!
   :drop-incoming! ref-drop-incoming!})

; TODO: Probably need to do something more here...
(defn- ref-set-revision! [layer rev]
  (dosync (alter (:props layer) :rev rev)))

(def ref-revision-impl
  {:set-revision! ref-set-revision!})

(defrecord RefLayer  [props nodes incoming])

(extend RefLayer
  jiraph.layer/Layer
  (merge mem-impl
         ref-impl)

  retro.core/WrappedTransactional
  mem-txn-impl

  retro.core/Revisioned
  (merge mem-revision-impl
         ref-revision-impl))

; An atom based implementation

(defn- atom-truncate!
  [layer]
  (reset! (:nodes layer) {})
  (reset! (:incoming layer) {}))

(defn- atom-set-property!
  [layer key val]
  (swap! (:props layer) assoc key val))

(defn- atom-add-node!
  [layer id attrs]
  (swap! (:nodes layer) assoc id attrs))

(defn- atom-append-node!
  [layer id attrs]
  (swap! (:nodes layer) update-in [id] merge attrs))

; Help me!  Could result in an inconsistent old/new pair.
(defn- atom-update-node!
  [layer id f args]
  (let [old (get @(:nodes layer) id)
        new (swap! (:nodes layer) update-in [id] f args)]
    [old new]))

(defn- atom-delete-node!
  [layer id]
  (swap! (:nodes layer) dissoc id))

(defn- atom-add-incoming!
  [layer id from-id]
  (swap! (:incoming layer)
         (fn [old]
           (let [incoming-set (conj (get old id #{}) from-id)]
             (assoc old id incoming-set)))))

(defn- atom-drop-incoming!
  [layer id from-id]
  (swap! (:incoming layer) update-in [id] disj from-id))

(def atom-impl
  {:truncate!      atom-truncate!
   :set-property!  atom-set-property!
   :add-node!      atom-add-node!
   :append-node!   atom-append-node!
   :update-node!   atom-update-node!
   :delete-node!   atom-delete-node!
   :add-incoming!  atom-add-incoming!
   :drop-incoming! atom-drop-incoming!})

(defn- atom-set-revision! [layer rev]
  (swap! (:props layer) assoc :rev rev))

(def atom-revision-impl
  {:set-revision! atom-set-revision!})

(defrecord AtomLayer [props nodes incoming])

(extend AtomLayer
  jiraph.layer/Layer
  (merge mem-impl
         atom-impl)

  retro.core/WrappedTransactional
  mem-txn-impl

  retro.core/Revisioned
  (merge mem-revision-impl
         atom-revision-impl))

(defn ref-layer
  []
  (RefLayer. (ref {}) (ref {}) (ref {})))

(defn atom-layer
  []
  (AtomLayer. (atom {}) (atom {}) (atom {})))

