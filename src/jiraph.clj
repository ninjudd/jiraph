(ns jiraph
  (:use ninjudd.utils)
  (:use cupboard.bdb.je)
  (:use clojure.contrib.java-utils))

(defclass Graph :env :nodes-db :edges-db)
(defclass Node  :id :type)
(defclass Edge  :from-id :to-id :type)

(defn open-graph [path]
  (let [env      (db-env-open (file path) :allow-create true :transactional true)
        nodes-db (db-open env "nodes"     :allow-create true :transactional true)
        edges-db (db-open env "edges"     :allow-create true :transactional true)]
    (Graph [env nodes-db edges-db])))

(defn add-node! [graph & args]
  (let [node (Node args)]
    (db-put (graph :nodes-db) 
            (:id node) node 
            :no-overwrite true)))

(defn delete-node! [graph id]
  (db-delete (graph :nodes-db) id))

(defn get-node [graph id]
  (let [[key node] (db-get (graph :nodes-db) id)]
    node))

(defn assoc-node! [graph & args] ; modify an existing node
  (let [attrs  (Node args)
        id     (:id attrs)
        node (get-node graph id)]
    (if node
      (db-put (graph :nodes-db)
              id (merge node attrs))
      nil)))

(defn edge-key [edge]
  (map edge '(:from-id :type :to-id)))

(defn add-edge! [graph & args]
  (let [edge (Edge args)]
    (db-put (graph :edges-db)
            (edge-key edge) edge 
            :no-overwrite true)))

(defn delete-edge! [graph from-id to-id type]
  (db-delete (graph :edges-db) (list from-id type to-id)))

(defn- get-edges* [graph key]
  (let [n   (count key)
        key (into (repeat (- 3 n) nil) key)] ; must be length 3
    (with-db-cursor [cursor (graph :edges-db)]
      (loop [[k v] (db-cursor-search cursor key)
             edges []]
        (if (= (take n k) (take n key))
          (recur (db-cursor-next cursor) (conj edges v))
          edges)))))

(defn get-edge [graph from-id to-id type]
  (first (get-edges* graph (list from-id type to-id nil))))

(defn get-edges
  ([graph from-id type] (get-edges* graph (list from-id type)))
  ([graph from-id]      (get-edges* graph (list from-id))))

(defn assoc-edge! [graph & args] ; modify an existing edge
  (let [attrs (Edge args)
        key   (edge-key attrs)
        edge  (first (get-edges* graph key))]
    (if edge
      (db-put (graph :edges-db)
              key (merge edge attrs))
      nil)))

(defclass Walk :graph :focus-id :steps :nodes)
(defclass Step :source :edge :node)

(defn walk-node [walk id]
  (or (get-in walk [:nodes id])
      (get-node (walk :graph) id)))

(defn walk-step
  ([walk edge]
     (apply walk-step walk (map edge '(:to-id :from-id :type))))
  ([walk from-id to-id type]
     (get-in walk [:steps to-id from-id type])))

(defn walked? [walk edge]
  (not (nil? (walk-step walk edge))))

(defn follow [walk from-id step]
  (let [graph (walk :graph)]
    (map (fn [edge]
           (let [to-id (edge :to-id)
                 node  (walk-node walk to-id)]
             (Step [step edge node])))
         (get-edges graph from-id))))

(defn full-walk [graph focus-id]
  (let [node (get-node graph focus-id)]
    (if (nil? node)
      nil
      (loop [walk      (Walk [graph focus-id {} {focus-id node}])
             to-follow (queue (follow walk focus-id nil))]
        (if (empty? to-follow)
          walk
          (let [step    (first to-follow)
                edge    (step :edge)
                node    (step :node)
                from-id (edge :from-id)
                to-id   (edge :to-id)
                type    (edge :type)
                to-follow (pop to-follow)]
            (if (walked? walk edge)
              (recur walk to-follow)
              (recur (-> walk
                         (assoc-in [:steps to-id from-id type] step)
                         (assoc-in [:nodes to-id] node))
                     (into to-follow (follow walk to-id step))))))))))
