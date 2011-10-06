(ns jiraph.stm-layer2
  (refer-clojure :exclude [meta])
  (:use [jiraph.layer :only [Enumerate Counted Optimized Basic Layer LayerMeta]]
        [retro.core   :only [*revision* WrappedTransactional Revisioned]]))

(def empty-store {:revisions (sorted-map-by >)})

(defn current-rev [layer]
  (or (:revision layer)
      (first (keys (:store layer)))))

(defn now [layer]
  (-> layer :store deref (get (current-rev layer))))

(def nodes (comp :nodes now))
(def meta (comp :meta now))

(defrecord STMLayer [store revision]
  Enumerate
  (node-id-seq [this]
    (-> this nodes keys))
  (node-seq [this]
    (-> this nodes seq))

  Counted
  (node-count [this]
    (-> this nodes count))

  LayerMeta
  (get-layer-meta [this k]
    (-> this meta (get k)))
  (assoc-layer-meta [this k v]
    (alter (:store this)
           assoc-in [:scratch :meta k] v))

  Basic
  (get-node [this k]
    (-> this nodes (get k)))
  (assoc-node [this k v]
    (alter (:store this)
           assoc-in [:scratch :nodes k] v))
  (dissoc-node [this k]
    (alter (:store this)
           update-in [:scratch :nodes] dissoc k))

  Optimized
  (query-fn [this ks f]
    nil)
  (update-fn [this ks f]
    (fn [& args]
      (alter (:store this)
             #(apply update-in % (concat [:scratch :nodes] ks) f args))))

  Revisioned
  (get-revisions [this _] ;; we store all nodes at every revision
    (map key (subseq @(:store this) <= (current-rev this))))
  (at-revision [this rev]
    (STMLayer. (:store this) rev))
  (current-revision [this]
    (current-rev this))

  WrappedTransactional
  (txn-wrap [this f]
    #(dosync
      (alter (:store this)
             (fn [store]
               (assoc store
                 :scratch (now this))))
      (let [ret (f)]
        (alter (:store this)
               (fn [store]
                 (-> store
                     (assoc (inc (current-rev this)) (:scratch store))
                     (dissoc :scratch))))
        ret)))

  Layer
  (open [this] nil)
  (close [this] nil)
  (sync! [this] nil)
  (optimize! [this] nil)
  (truncate! [this]
    (ref-set (:store this) empty-store))
  (node-exists? [this id]
    (-> this nodes (get id) boolean)))
