(ns jiraph.graph
  (:use [useful :only [into-map]])
  (:require [jiraph.layer :as layer]))

(def *graph* nil)

(defmacro with-graph [graph & forms]
  `(binding [*graph* ~graph]
     (try (dorun (map layer/open (vals *graph*)))
          ~@forms
          (finally (dorun (map layer/close (vals *graph*)))))))

(defmacro at-revision [rev & forms]
  `(binding [layer/*rev* ~rev]
     ~@forms))

(defmacro transaction [layer & forms]
  `(layer/transaction (*graph* ~layer) ~@forms))

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
  (transaction layer
    (let [val (get-property layer key)]
      (set-property! layer key (apply f val args)))))

(defn get-node     [layer id] (layer/get-node     (*graph* layer) id))
(defn get-meta     [layer id] (layer/get-meta     (*graph* layer) id))
(defn node-exists? [layer id] (layer/node-exists? (*graph* layer) id))

(defn add-node!    [layer id & attrs]  (layer/add-node!    (*graph* layer) id (into-map attrs)))
(defn append-node! [layer id & attrs]  (layer/append-node! (*graph* layer) id (into-map attrs)))
(defn assoc-node!  [layer id & attrs]  (layer/assoc-node!  (*graph* layer) id (into-map attrs)))
(defn update-node! [layer id f & args] (layer/update-node! (*graph* layer) id f args))

(defn compact-node! [layer id] (layer/compact-node! (*graph* layer) id))
(defn delete-node!  [layer id] (layer/delete-node!  (*graph* layer) id))

(defn layers []
  (keys *graph*))

(defn all-revisions
  ([meta]
     (filter pos? (:rev meta)))
  ([layer id]
     (all-revisions (get-meta layer id))))

(defn revisions
  ([meta]
     (reverse
      (take-while pos? (reverse (:rev meta)))))
  ([layer id]
     (revisions (get-meta layer id))))

(defn incoming
  ([meta]
     (:in meta))
  ([layer id]
     (:in (get-meta layer id))))
