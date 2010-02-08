(ns jiraph.graph
  (:use jiraph.utils)
  (:use clojure.contrib.java-utils)
  (:use protobuf)
  (:use jiraph.tc))

(defmacro defgraph [sym & args]
  (let [layer? #(and (list? %) (= 'layer (first %)))
        layers (filter layer? args)
        opts   (eval (apply hash-map (remove layer? args)))
        open-layer
        (fn [graph [_ layer & args]]
          (let [opts (merge opts (apply hash-map args))]
            (assoc graph layer
              (db-open (-> opts
                           (assoc :path (str (opts :path) "/" (name layer)))
                           (assoc-if (opts :proto)
                             :dump protobuf-bytes
                             :load (partial protobuf (opts :proto))))))))]
    `(def ~sym
       ~(reduce open-layer {} layers))))

(defn open-graph [& args]
  (let [opts (args-map args)]
    (db-open (assoc opts :val-fn protobuf-bytes))))

(defn make-node [layer & args]
  (if (:proto layer)
    (apply protobuf (:proto layer) args)
    (apply hash-map args)))

(defn add-node! [graph layer id & args]
  (let [layer (graph layer)
        node  (apply make-node layer :id id args)]
    (db-add layer id node)))

(defn delete-node! [graph layer id]
  (db-delete (graph layer) id))

(defn get-node [graph layer id]
  (db-get (graph layer) id))

(defn update-node! [graph layer id update & args]
  (db-update (graph layer) id
    (fn [old]
      (if (fn? update) (apply update old args) update))))

(defn assoc-node! [graph layer id & args]
  (let [layer (graph layer)]
    (update-node! layer id
      (fn [node]
        (if node
          (merge node (apply hash-map args)))))))

(defn append-node!
  "this is more efficient than update-node! or assoc-node! because appending a protobuf to
   another merges them and appends repeated elements."
  [graph layer id & args]
  (let [layer (graph layer)]
    (verify (:proto layer) "cannot append unless you are using protocol buffers"
      (db-append layer id (apply make-node layer args)))))

(defn add-edge [node & args]
  (let [edges (or (node :edges) [])
        edge  (apply hash-map args)]
    (assoc node :edges (conj edges edge))))

(defn add-edge! [graph layer id & args]
  (let [edge (apply hash-map args)]
    (if (:proto (graph layer))
      (append-node! graph layer id :edges [edge])
      (update-node! graph layer id add-edge args))
    edge))

(defn remove-edges [node pred]
  (let [edges (or (node :edges) [])]
    (assoc node :edges (remove pred edges))))

(defn get-edges [graph layer id]
  (or (:edges (get-node graph layer id)) []))