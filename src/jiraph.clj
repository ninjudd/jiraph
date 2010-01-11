(ns jiraph
  (:use cupboard.bdb.je cupboard.utils)
  (:use clojure.contrib.java-utils))

(defstruct graph        :env :vertices-db :edges-db)
(defstruct graph-vertex :id :type)
(defstruct graph-edge   :from-id :to-id :type)

(defn- to-struct [s args]
  (if (and (sequential? args)
           (= (count args) 1)
           (map? (first args)))
    (merge (struct-map s) (first args))
    (apply struct-map s args)))

(defn open-graph [path]
  (let [env         (db-env-open (file path) :allow-create true :transactional true)
        vertices-db (db-open env "vertices"  :allow-create true :transactional true)
        edges-db    (db-open env "edges"     :allow-create true :transactional true)]
    (struct graph env vertices-db edges-db)))

(defn add-vertex! [graph & args]
  (let [vertex (to-struct graph-vertex args)]
    (db-put (graph :vertices-db) 
            (:id vertex) vertex 
            :no-overwrite true)))

(defn delete-vertex! [id]
  (db-delete (graph :vertices-db) id))

(defn vertex [graph id]
  (let [[key vertex] (db-get (graph :vertices-db) id)]
    vertex))

(defn assoc-vertex! [graph & args]
  (let [attrs  (to-struct graph-vertex args)
        id     (:id attrs)
        vertex (vertex graph id)]
    (if vertex
      (db-put (graph :vertices-db)
              id (merge vertex attrs))
      nil)))

(defn edge-key [edge]
  (map edge '(:from-id :type :to-id)))

(defn add-edge! [graph & args]
  (let [edge (to-struct graph-edge args)]
    (db-put (graph :edges-db)
            (edge-key edge) edge 
            :no-overwrite true)))

(defn delete-edge! [from-id to-id type]
  (db-delete (graph :edges-db) (list from-id type to-id)))

(defn- edges* [graph start-key]
  (let [n      (count start-key)
        cursor (db-cursor-open (graph :edges-db))]
    (loop [[key value] (db-cursor-search cursor start-key)
           edges       []]
      (if (= (take n key) (take n start-key))
        (recur (db-cursor-next cursor) (conj edges value))
        edges))))

(defn edge [graph from-id to-id type]
  (first (edges* graph (list from-id type to-id))))

(defn edges
  ([graph from-id type] (edges* graph (list from-id type)))
  ([graph from-id]      (edges* graph (list from-id))))

(defn assoc-edge! [graph & args]
  (let [attrs (to-struct graph-edge args)
        key   (edge-key attrs)
        edge  (first (edges* graph key))]
    (if edge
      (db-put (graph :edges-db)
              key (merge edge attrs))
      nil)))
