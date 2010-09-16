(ns jiraph.graph
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

(defn sync! [& layers]
  (if (empty? layers)
    (doseq [layer (vals *graph*)]
      (layer/sync! layer))
    (doseq [layer layers]
      (layer/sync! (*graph* layer)))))

(defmacro transaction [layer & forms]
  `(layer/transaction (*graph* ~layer) ~@forms))

(defn truncate!  [layer] (layer/truncate!  (*graph* layer)))
(defn node-ids   [layer] (layer/node-ids   (*graph* layer)))
(defn node-count [layer] (layer/node-count (*graph* layer)))

(defn get-node     [layer id] (layer/get-node     (*graph* layer) id))
(defn get-meta     [layer id] (layer/get-meta     (*graph* layer) id))
(defn node-exists? [layer id] (layer/node-exists? (*graph* layer) id))

(defn add-node!    [layer id attrs]    (layer/add-node!    (*graph* layer) id attrs))
(defn append-node! [layer id attrs]    (layer/append-node! (*graph* layer) id attrs))
(defn assoc-node!  [layer id attrs]    (layer/assoc-node!  (*graph* layer) id attrs))
(defn update-node! [layer id f & args] (layer/update-node! (*graph* layer) id f args))

(defn compact-node! [layer id] (layer/compact-node! (*graph* layer) id))
(defn delete-node!  [layer id] (layer/delete-node!  (*graph* layer) id))

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
