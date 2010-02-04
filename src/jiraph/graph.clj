(ns jiraph.graph
  (:use jiraph.utils)
  (:use clojure.contrib.java-utils)
  (:use protobuf)
  (:use jiraph.tc))

(defn open-graph [path & args]
  (let [opts (args-map args)]
    (db-open path (assoc opts :val-fn protobuf-bytes))))

(defn make-node [graph & args]
  (apply protobuf (graph :node-proto) args))

(defn make-edge-list [graph val]
  (let [proto (graph :edge-list-proto)]
    (if val
      (protobuf proto val)
      (protobuf proto))))

(defn add-node! [graph & args]
  (let [node (apply make-node graph args)]
    (db-add graph :nodes (:id node) node)))

(defn delete-node! [graph id]  
  (db-delete graph :nodes id))

(defn get-node [graph id]
  (let [val (db-get graph :nodes id)]
    (if val (make-node graph val))))

(defn update-node! [graph id update]
  (db-update graph :nodes id
    (fn [val]
      (let [old-node (if val (make-node graph val))
            new-node (if (fn? update) (update old-node) update)]
        (if new-node
          (verify (= id (new-node :id)) "cannot change id with update-node!"
            new-node))))))

(defn assoc-node! [graph & args]
  (let [attrs (apply hash-map args)]
    (update-node! graph (:id attrs)
      (fn [node]
        (if node
          (merge node attrs))))))

(defn get-edges [graph from-id type]
  (let [val (db-get graph :edges [from-id type])]
    (:edges (make-edge-list graph val))))

(defn- edge-index [edges to-id]
  (find-index #(= to-id (:to-id %)) edges))

(defn get-edge [graph from-id to-id type]
  (let [edges (get-edges graph from-id type)
        index (edge-index edges to-id)]
    (edges index)))

(defn update-edge! [graph from-id to-id type update]
  (db-update graph :edges [from-id type]
    (fn [val]
      (let [edge-list (make-edge-list graph val)
            edges     (or (edge-list :edges) [])
            index     (edge-index edges to-id)
            old-edge  (if index (edges index))
            new-edge  (if (fn? update) (update old-edge) update)]
        (assoc edge-list :edges
               (if (nil? new-edge)
                 ; remove the old edge
                 (remove-nth edges index)
                 (verify (and (= from-id (new-edge :from-id)) (= type (new-edge :type)))
                         "cannot change from-id or type with update-edge!"
                   (if (nil? old-edge)
                     ; add a new edge to the end of the list
                     (conj edges new-edge)
                     (let [new-to-id (new-edge :to-id)]
                       (verify (or (= to-id new-to-id) (nil? (edge-index edges new-to-id)))
                               "cannot create duplicate to-id with update-edge!"
                         ; replace the old edge with the new edge
                         (assoc edges index new-edge)))))))))))

(defn add-edge! [graph & args]
  (let [attrs   (apply hash-map args)
        from-id (attrs :from-id)
        to-id   (attrs :to-id)
        type    (attrs :type)]
    (update-edge! graph from-id to-id type
      (fn [edge]
        (verify (nil? edge) "edge already exists"
          attrs)))))

(defn delete-edge! [graph from-id to-id type]
  (update-edge! graph from-id to-id type nil))

(defn assoc-edge! [graph & args]
  (let [attrs   (apply hash-map args)
        from-id (attrs :from-id)
        to-id   (attrs :to-id)
        type    (attrs :type)]
    (update-edge! graph from-id to-id type
      #(merge % attrs))))