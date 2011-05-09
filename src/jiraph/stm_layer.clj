(ns jiraph.stm-layer
  (:use jiraph.layer
        [useful :only [update append]]))

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
  
  (set-property! [layer key val] (dosync (alter properties assoc key val)))

  (txn [layer f] (dosync f))

  (get-node [layer id] (get @data id))

  (get-meta [layer id] (get @meta id))

  (node-exists? [layer id] (contains? @data id))

  (add-node! [layer id attrs]
             (let [attrs (assoc attrs :id id)]
               (when-not (node-exists? layer id)
                 (dosync
                  (let [id-meta (get-meta layer id)]
                    (when-not (:in id-meta)
                      (alter meta assoc id (assoc id-meta :in #{}))))
                  (ref-set meta (update-incoming @meta id (:edges attrs)))
                  (alter data into {id attrs}))
                 attrs)))
  
  (update-node! [layer id f args]
                (dosync (let [old (get-node layer id)
                              new (assoc (apply f old args) :id id)]
                          (alter data assoc id new)
                          (get-node layer id))))
  
  (assoc-node! [layer id attrs]
               (update-node! layer id merge attrs))
  
  (append-node! [layer id attrs]
                (when-not (empty? attrs)
                  (dosync
                      (alter data append {id attrs})
                      (ref-set meta (update-incoming @meta id (:edges attrs))))
                  (assoc attrs :id id))))

(defn make []
  (STMLayer. (ref {}) (ref {}) (ref {})))