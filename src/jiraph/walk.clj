(ns jiraph.walk
  (:use jiraph.graph)
  (:use jiraph.utils))

(defclass Walk :graph :focus-id :steps :nodes :include? :ids :count :to-follow)
(defclass Step :source :from-id :to-id :layer :edge)

(defn lookup-node [walk layer id]
  (or (@(walk :nodes) [id layer])
      (let [node (get-node (walk :graph) layer id)]
        (swap! (walk :nodes) assoc! [id layer] node)
        node)))

(defn walked? [walk step]
  (not (nil? (get-in walk [:steps (step :to-id) (step :from-id) (step :layer)]))))

(defn back? [step]
  (= (step :to-id)
     (get-in step [:source :from-id])))

(defn add-node [walk id]
  (if (get-in walk [:include? id])
    walk
    (-> walk
        (update-in [:ids] conj id)
        (update-in [:count] inc)
        (update-in [:include?] assoc id true))))

(defn empty-walk [graph focus-id]
  (let [step (Step :to-id focus-id)]
    (Walk :graph     graph
          :focus-id  focus-id
          :steps     {}
          :nodes     (atom {})
          :include?  {}
          :ids       []
          :count     0
          :to-follow (queue step))))

(defn assoc-step [walk step]
  (if (or (back? step) (walked? walk step))
    walk
    (let [to-id (step :to-id)]
      (-> walk
          (add-node to-id)
          (assoc-in  [:steps to-id (step :from-id) (step :layer)] step)
          (update-in [:to-follow] conj step)))))

(defn follow [walk layer step]
  (let [from-id (step :to-id)
        node    (lookup-node walk layer from-id)
        edges   (if node (node :edges) [])
        make-step
        (fn [edge]
          (let [to-id (edge :to-id)]
            (Step [step from-id to-id layer edge])))]
    (reduce assoc-step walk
      (map make-step edges))))

(defn walk [graph focus-id limit]
  (loop [walk (empty-walk graph focus-id)]
    (let [step (-> walk :to-follow first)
          walk (update-in walk [:to-follow] pop)]     
      (if (or (nil? step) (< limit (walk :count)))
        (do (println (count (walk :to-follow)))
            walk)
        (recur (follow walk :tree step))))))