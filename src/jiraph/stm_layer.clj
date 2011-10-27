(ns jiraph.stm-layer
  (:refer-clojure :exclude [meta])
  (:use [jiraph.layer :only [Enumerate Counted Optimized Basic Layer LayerMeta
                             ChangeLog skip-applied-revs max-revision get-revisions close]]
        [jiraph.graph :only [*skip-writes*]]
        [retro.core   :only [WrappedTransactional Revisioned
                             get-queue at-revision current-revision empty-queue]]
        [useful.fn    :only [given fix]]
        [useful.utils :only [returning]])
  (:import (java.io FileNotFoundException)))

(comment
  The STM layer stores a single ref, pointing to a series of whole-layer
  snapshots over time, one per committed revision. Each snapshot contains
  a :nodes section and a :meta section - the meta is managed entirely by
  jiraph.graph. Note that while metadata is revisioned, it does not keep a
  changelog like nodes do, so you can't meaningfully read metadata from a
  revision in the future (which you can do for nodes).

  The layer also tracks a current revision, to allow any snapshot to be
  viewed as if it were the full graph.

  So a sample STM layer might look like the following.

  {:revision 2
   :store (ref {0 {}
                1 {:meta {"some-key" "some-value"}
                   :nodes {"profile-4" {:name "Rita" :edges {"profile-9" {:rel :child}}}}}
                2 {:meta {"some-key" "other-value"}
                   :nodes {"profile-4" {:name "Rita" :edges {"profile-9" {:rel :child}}
                                        :age 39}
                           "profile-9" {:name "William"}}}})})

(def empty-store (sorted-map-by > 0 {}))

(defn current-rev [layer]
  (or (:revision layer) ;; revision explicitly set
      (first (keys (-> layer :store deref))))) ;;  get most recent revision

(defn now [layer]
  (-> layer :store deref (get (current-rev layer))))

(def ^{:private true} nodes (comp :nodes now))
(def ^{:private true} meta (comp :meta now))

(defrecord STMLayer [store revision filename]
  Object
  (toString [this]
    (pr-str this))

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
    (if-not revision
      (-> this meta (get k))

      ;; >= is "backward" because we're storing the map sorted in descending order
      ;; really this finds the first revision less than or equal to current
      (let [[_ store] (? (first (subseq @store >= (? revision))))]
        (get-in store [:meta k]))))
  (assoc-layer-meta! [this k v]
    (alter store ;; TODO make this work when no revision?
           assoc-in [(current-rev this) :meta k] v))

  Basic
  (get-node [this k not-found]
    (let [n (-> this nodes (get k not-found))]
      (if-not (identical? n not-found)
        n
        (let [touched-revisions (get-revisions (at-revision this nil) k)
              curr (current-revision this)
              most-recent (or (first (if curr
                                       (drop-while #(> % curr) touched-revisions)
                                       touched-revisions))
                              0)]
          (-> @store (get-in [most-recent :nodes k] not-found))))))
  (assoc-node! [this k v]
    (alter store
           assoc-in [(current-rev this) :nodes k] v))
  (dissoc-node! [this k]
    (alter store
           update-in [(current-rev this) :nodes] dissoc k))

  Revisioned
  (at-revision [this rev]
    (assoc this :revision rev))
  (current-revision [this]
    (current-rev this))

  WrappedTransactional
  (txn-wrap [_ f]
    (fn [^STMLayer layer]
      (dosync
       (let [rev (.revision layer)
             max-rev (max-revision layer)]
         (cond (nil? rev) (f layer)
               (< rev max-rev) (binding [*skip-writes* true] (f (empty-queue layer)))
               :else
               (let [store (.store layer)
                     prev (get @store max-rev)
                     rev (inc revision)]
                 (alter store fix
                        #(not (contains? % rev))
                        #(assoc % rev prev))
                 (returning (f (at-revision layer rev))
                   (alter store fix
                          #(identical? prev (get % rev))
                          #(dissoc % rev)))))))))

  Layer
  (open [this]
    (when filename
      (dosync
       (try
         (doto this
           (-> :store (ref-set (read-string (slurp filename)))))
         (catch FileNotFoundException e this)))))
  (close [this]
    (when filename
      (spit filename @store)))
  (sync! [this]
    (close this))
  (optimize! [this] nil)
  (truncate! [this]
    (dosync ;; since this should only be called outside a retro transaction
     (ref-set (:store this) empty-store)))
  (node-exists? [this id]
    (-> this nodes (get id) boolean)))

(defn make
  ([] (make nil))
  ([filename] (STMLayer. (ref empty-store) nil filename)))
