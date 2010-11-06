(ns jiraph.graph
  (:use [useful :only [into-map update remove-keys-by-val remove-vals]])
  (:require [jiraph.layer :as layer]
            [jiraph.tokyo-database :as tokyo]
            [jiraph.byte-append-layer :as byte-append-layer]))

(def ^:dynamic *graph* nil)

(defn open! []
  (dorun (map layer/open (vals *graph*))))

(defn close! []
  (dorun (map layer/close (vals *graph*))))

(defmacro with-graph [graph & forms]
  `(binding [*graph* ~graph]
     (try (open!)
          ~@forms
          (finally (close!)))))

(defmacro with-graph! [graph & forms]
  `(let [graph# *graph*]
     (alter-var-root #'*graph* (fn [_#] ~graph))
     (try (open!)
          ~@forms
          (finally (close!)))
     (alter-var-root #'*graph* (fn [_#] graph#))))

(defmacro at-revision [rev & forms]
  `(binding [layer/*rev* ~rev]
     ~@forms))

(defmacro with-each-layer [layers & forms]
  `(doseq [~'layer (if (empty? ~layers) (vals *graph*) (map *graph* ~layers))]
     ~@forms))

(defn sync! [& layers]
  (with-each-layer layers
    (layer/sync! layer)))

(defn truncate! [& layers]
  (with-each-layer layers
    (layer/truncate! layer)))

(defn node-ids   [layer] (layer/node-ids   (*graph* layer)))
(defn node-count [layer] (layer/node-count (*graph* layer)))

(defn get-property  [layer key]     (layer/get-property  (*graph* layer) key))
(defn set-property! [layer key val] (layer/set-property! (*graph* layer) key val))

(defn update-property! [layer key f & args]
  (layer/with-transaction (*graph* layer)
    (let [val (get-property layer key)]
      (set-property! layer key (apply f val args)))))

(defn current-revision [& layers]
  (apply max 0 (for [layer (if (empty? layers) (keys *graph*) layers)]
                 (or (get-property layer :rev) 0))))

(defn get-node     [layer id] (layer/get-node     (*graph* layer) id))
(defn node-exists? [layer id] (layer/node-exists? (*graph* layer) id))

(defn append-only? [layer]
  (let [append-only (:append-only (meta *graph*))]
    (or (true? append-only)
        (contains? append-only layer))))

(defn add-node! [layer id & attrs]
  {:pre [(if (append-only? layer) layer/*rev* true)]}
  (let [layer (*graph* layer)
        node  (layer/add-node! layer id (into-map attrs))]
    (doseq [[to-id edge] (:edges node)]
      (when-not (:deleted edge)
        (layer/add-incoming! layer to-id id)))
    node))

(defn append-node! [layer id & attrs]
  {:pre [(if (append-only? layer) layer/*rev* true)]}
  (let [layer (*graph* layer)
        node  (layer/append-node! layer id (into-map attrs))]
    (doseq [[to-id edge] (:edges node)]
      (if (:deleted edge)
        (layer/drop-incoming! layer to-id id)
        (layer/add-incoming!  layer to-id id)))
    node))

(defn update-node! [layer id f & args]
  {:pre [(not (append-only? layer))]}
  (let [layer     (*graph* layer)
        [old new] (layer/update-node! layer id f args)]
    (let [new-edges (set (remove-keys-by-val :deleted (:edges new)))
          old-edges (set (remove-keys-by-val :deleted (:edges old)))]
      (doseq [to-id (remove old-edges new-edges)]
        (layer/add-incoming! layer to-id id))
      (doseq [to-id (remove new-edges old-edges)]
        (layer/drop-incoming! layer to-id id)))
    [old new]))

(defn delete-node! [layer id]
  (let [layer (*graph* layer)
        node  (layer/delete-node! layer id)]
    (doseq [[to-id edge] (:edges node)]
      (if-not (:deleted edge)
        (layer/drop-incoming! layer to-id id)))))

(defn assoc-node!
  "Associate attrs with a node."
  [layer id & attrs]
  (apply update-node! layer id merge attrs))

(defn compact-node!
  "Compact a node by removing deleted edges. This will also collapse appended revisions."
  [layer id]
  (update-node! layer id update :edges (partial remove-vals :deleted)))

(defn layers []
  (keys *graph*))

(defn get-all-revisions [layer id]
  (filter pos? (layer/get-revisions (*graph* layer) id)))

(defn get-revisions [layer id]
  (reverse
   (take-while pos? (reverse (layer/get-revisions (*graph* layer) id)))))

(defn get-incoming [layer id] (layer/get-incoming (*graph* layer) id))

(defn field-to-layer [graph layers]
  (reduce (fn [m layer]
            (reduce #(assoc %1 %2 layer)
                    m (layer/fields (graph layer))))
          {} (reverse layers)))

(defn- skip-past-revisions! [layer f]
  (let [rev (or (get-property layer :rev) 0)]
    (if (<= layer/*rev* rev)
      (printf "skipping revision: revision [%s] <= current revision [%s]\n" layer/*rev* rev)
      (let [result (f)]
        (if layer/*rev*
          (set-property! layer :rev layer/*rev*))
        result))))

(defn transaction [layer f]
  (try (layer/with-transaction (*graph* layer)
         (if layer/*rev*
           (skip-past-revisions! layer f)
           (f)))
     (catch javax.transaction.TransactionRolledbackException e)))

(defmacro with-transaction [layer & forms]
  `(transaction ~layer (fn [] ~@forms)))

(defn abort-transaction []
  (throw (javax.transaction.TransactionRolledbackException.)))

(defn layer [path]
  (byte-append-layer/make (tokyo/make {:path path :create true})))