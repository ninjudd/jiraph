(ns jiraph.stm-layer
  (:use jiraph.layer
        retro.core
        [useful :only [update adjoin]]))

(defn conj-set
  "Conj onto collection ensuring it is a set."
  [coll item]
  (conj (set coll) item))

(defn update-incoming [meta from-id edges]
  (reduce (fn [meta [to-id edge]]
            (if (:deleted edge)
              (update-in meta [to-id :in] disj from-id)
              (update-in meta [to-id :in] conj-set from-id)))
          meta
          edges))

(defn destroy-incoming
  "Get rid of all incoming edges on an id."
  [meta from-id edges]
  (reduce (fn [meta to-id] (update-in meta [to-id :in] disj from-id)) meta edges))

(defn get-meta [layer id] (get @(.meta layer) id))

(defn reverse-compare
  "Just like compare, but reversed."
  [x y] (cond (> x y) -1 (= x y) 0 (< x y) 1))

(def reverse-sorted-map
     ^{:doc "Returns a sorted-map that is sorted by its keys from highest to lowest."}
     (partial sorted-map-by reverse-compare))

(defn flip [f & args] (apply f (reverse args)))

(defn modify-rev
  "Modify a revision."
  [layer id node]
  (get-in
   (alter (.meta layer)
          adjoin
          (adjoin
           {id {:revs {*revision*
                       (binding [*revision* nil]
                         (dissoc (get-node layer id) :id))}
                :all-revs [*revision*]}}
           {id {:revs {*revision* node}}}))
   [id :revs *revision*]))

(defn drop-neg
  "Drop a negative number onto the end of the all-revs seq."
  [layer id] (alter (.meta layer) update-in [id :all-revs] conj -1))

(defn wipe-revs
  "Collapse all of an id's revisions."
  [layer id] (alter (.meta layer) assoc-in [id :revs] (reverse-sorted-map)))

(defn initiate-revs
  "Create an initial sorted map for :revs if one doesn't yet exist."
  [layer id]
  (when-not (:revs (get-meta layer id)) (wipe-revs layer id)))

(defn get-rev
  "Fetch a particular revision of an id."
  [layer id]
  (->> *revision* (subseq (:revs (get-meta layer id)) >=) first second))

(defn- make-node [attrs]
  (let [attrs (dissoc attrs :id)]
    (if *revision*
      (assoc attrs :rev *revision*)
      (dissoc attrs :rev))))

(deftype STMLayer [data meta properties]
  jiraph.layer/Layer

  (open [layer] nil)
  (close [layer] nil)
  (sync! [layer] nil)

  (truncate! [layer] (dosync (ref-set data {})
                             (ref-set meta {})
                             (ref-set properties {})))

  (node-count [layer] (count @data))

  (node-ids [layer] (keys @data))

  (get-property [layer key] (get @properties key))
  
  (set-property! [layer key val] (dosync ((alter properties assoc key val) key)))

  (get-node [layer id]
            (when-let [rev (if *revision* (get-rev layer id) (get @data id))]
              (assoc rev :id id)))

  (node-exists? [layer id] (contains? @data id))

  (add-node! [layer id attrs]
             (let [node (make-node attrs)]
               (when-not (node-exists? layer id)
                 (dosync
                  (initiate-revs layer id)
                  (let [id-meta (get-meta layer id)]
                    (when-not (:in id-meta)
                      (alter meta assoc id (assoc id-meta :in #{}))))
                  (ref-set meta (update-incoming @meta id (:edges node)))
                  (when *revision* (modify-rev layer id node))
                  (alter data into {id node}))
                 node)))
  
  (update-node! [layer id f args]
                (dosync
                 (let [old (get-node layer id)
                       new (make-node (assoc (apply f old args) :id id))]
                   (wipe-revs layer id)
                   (drop-neg layer id)
                   (ref-set meta (destroy-incoming @meta id (:in (get-meta layer id))))
                   (ref-set meta (update-incoming @meta id (:edges new)))
                   (alter data assoc id new)
                   (map #(dissoc % :id) [old new]))))
  
  (append-node! [layer id attrs]
                (let [node (make-node attrs)]
                  (when-not (empty? attrs)
                    (dosync
                     (initiate-revs layer id)
                     (let [old (dissoc (get-node layer id) :id)
                           return (if *revision* (modify-rev layer id node) node)]
                       (alter data adjoin {id node})
                       (ref-set meta (update-incoming @meta id (:edges node)))
                       node)))))

  (get-revisions [layer id] (-> layer (get-meta id) :all-revs))

  (get-incoming [layer id] (-> layer (get-meta id) :in))

  (add-incoming! [layer id from-id]
                 (dosync (ref-set meta (update-incoming @meta id {from-id true}))))

  (drop-incoming! [layer id from-id]
                  (dosync (ref-set meta (update-incoming @meta id {from-id false}))))

  retro.core/Revisioned
  
  (get-revision [layer] (get-property layer :rev))

  (set-revision! [layer rev] (set-property! layer :rev rev))

  retro.core/WrappedTransactional

  (txn-wrap [layer f] #(dosync (f))))

(defn make []
  (STMLayer. (ref {}) (ref {}) (ref {}))) 