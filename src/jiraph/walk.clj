(ns jiraph.walk
  (:use jiraph.graph)
  (:use jiraph.utils))

(defclass Walk :graph :focus-id :steps :nodes :node-ids :node-count :to-follow)
(defclass Step :id :source :edge :node)

(defn empty-walk [graph focus-id]
  (let [node (get-node graph focus-id)
        edge {:to-id focus-id :type :initial}
        step (Step [focus-id nil edge node])]
    (Walk :graph      graph
          :focus-id   focus-id
          :steps      {}
          :nodes      {}
          :node-ids   []
          :node-count 0
          :to-follow  (queue step))))

(defn node-walked? [walk node]
  (not (nil? (get-in walk [:nodes (node :id)]))))

(defn edge-walked? [walk edge]
  (not (nil? (get-in walk [:steps (edge :to-id) (edge :from-id) (edge :type)]))))

(defn step-walked? [walk step]
  (edge-walked? walk (step :edge)))

(defn assoc-node [walk node]
  (if (node-walked? walk node)
    walk
    (let [id (node :id)]
      (-> walk
          (assoc-in  [:nodes id] node)
          (update-in [:node-ids] conj id)
          (update-in [:node-count] inc)))))

(defn step-back? [step]
  (= (get-in step [:edge :to-id])
     (get-in step [:source :edge :from-id])))

(defn follow [walk step]
  (let [edges (get-edges (walk :graph) (step :id) :tree)]
    (map (fn [edge]
           (let [id   (edge :to-id)
                 node (or (get-in walk [:nodes id])
                          (get-node (walk :graph) id))]
             (Step [id step edge node])))
         edges)))

(defn assoc-step [walk step]
  (let [node    (step :node)
        edge    (step :edge)
        from-id (edge :from-id)
        to-id   (edge :to-id)
        type    (edge :type)]
    (-> walk
        (assoc-in  [:steps to-id from-id] step)
        (update-in [:to-follow] into (follow walk step))
        (assoc-node node))))

(defn walk [graph focus-id limit]
  (loop [walk (empty-walk graph focus-id)]
    (let [step (-> walk :to-follow first)
          walk (update-in walk [:to-follow] pop)]      
      (if (nil? step)
        walk
        (if (< limit (walk :node-count))
          walk
          (if (or (step-back? step) (step-walked? walk step))
            (recur walk)
            (recur (assoc-step walk step))))))))
