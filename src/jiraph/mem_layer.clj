(ns jiraph.mem-layer
  (:use jiraph.layer
        [retro.core :as retro]))

(deftype MemoryLayer
  [props nodes incoming]
  jiraph.layer/Layer
  (open      [layer])
  (close     [layer])
  (sync!     [layer])
  (optimize! [layer])
  (truncate!
    [layer]
    (dosync
      (ref-set nodes {})
      (ref-set incoming {})))

  (node-count       [layer] (count @nodes))
  (node-ids         [layer] (keys @nodes))
  (fields           [layer] '())
  (get-property     [layer key] (get @props key))
  (set-property!    [layer key val] (dosync (alter props assoc key val)))
  (get-node         [layer id] (get @nodes id))
  (node-exists?     [layer id] (contains? @nodes id))
  (add-node!        [layer id attrs] (dosync (alter nodes assoc id attrs)))
  (append-node!
    [layer id attrs]
    (dosync (alter nodes update-in [id] merge attrs)))
  (update-node!
    [layer id f args]
    (dosync 
      (let [old (get @nodes id)
            new (apply f old args)]
      (alter nodes assoc id new)
        [old new])))
  (delete-node!     [layer id] (dosync (alter nodes dissoc id)))
  (get-revisions    [layer id] id)
  (get-incoming     [layer id] (get @incoming id))
  (add-incoming!
    [layer id from-id]
    (dosync (alter incoming assoc id
                   (conj (get @incoming id #{}) from-id))))
  (drop-incoming!
    [layer id from-id]
    (dosync (alter incoming update-in [id] disj from-id)))

  retro.core/WrappedTransactional

  (txn-wrap [layer f] #(f))

  retro.core/Revisioned

  (get-revision [layer]
    (get @props :rev))

  (set-revision! [layer rev]
    (dosync (alter props :rev rev))))

(defn mem-layer
  []
  (MemoryLayer. (ref {}) (ref {}) (ref {})))

