(ns jiraph
  (:use ninjudd.utils)
  (:use cupboard.bdb.je)
  (:use clojure.contrib.java-utils))

(defclass Graph  :env :vertices-db :edges-db)
(defclass Vertex :id :type)
(defclass Edge   :from-id :to-id :type)

(defn open-graph [path]
  (let [env         (db-env-open (file path) :allow-create true :transactional true)
        vertices-db (db-open env "vertices"  :allow-create true :transactional true)
        edges-db    (db-open env "edges"     :allow-create true :transactional true)]
    (Graph [env vertices-db edges-db])))

(defn add-vertex! [graph & args]
  (let [vertex (Vertex args)]
    (db-put (graph :vertices-db) 
            (:id vertex) vertex 
            :no-overwrite true)))

(defn delete-vertex! [graph id]
  (db-delete (graph :vertices-db) id))

(defn get-vertex [graph id]
  (let [[key vertex] (db-get (graph :vertices-db) id)]
    vertex))

(defn assoc-vertex! [graph & args] ; modify an existing vertex
  (let [attrs  (Vertex args)
        id     (:id attrs)
        vertex (get-vertex graph id)]
    (if vertex
      (db-put (graph :vertices-db)
              id (merge vertex attrs))
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

(defclass Walk :focus-id :steps :vertices)
(defclass Step :id :source :edge)

(defn walk-vertex [walk id]
  (get-in walk [:vertices id]))

(defn walk-step
  ([walk edge]
     (apply walk-step walk (map edge '(:to-id :from-id :type))))
  ([walk from-id to-id type]
     (get-in walk [:steps to-id from-id type])))

(defn walked? [walk edge]
  (not (nil? (walk-step walk edge))))

(defn follow [graph id step]
  (map #(Step [id step %]) (get-edges graph id)))

(defn full-walk [graph focus-id]
  (let [vertex (get-vertex graph focus-id)]
    (if (nil? vertex)
      nil
      (loop [walk      (Walk [focus-id {} {focus-id vertex}])
             to-follow (queue (follow graph focus-id nil))]
        (if (empty? to-follow)
          walk
          (let [step      (first to-follow)
                edge      (step :edge)
                from-id   (edge :from-id)
                to-id     (edge :to-id)
                type      (edge :type)
                vertex    (or (walk-vertex walk to-id) (get-vertex graph to-id))
                to-follow (pop to-follow)]
            (if (walked? walk edge)
              (recur walk to-follow)
              (recur (-> walk
                         (assoc-in [:steps to-id from-id type] step)
                         (assoc-in [:vertices to-id] vertex))
                     (into to-follow (follow graph to-id step))))))))))
