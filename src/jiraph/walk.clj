(ns jiraph.walk
  (:use jiraph.graph)
  (:use jiraph.utils))

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
        (update-in! [:ids] conj! id)
        (update-in! [:count] inc)
        (assoc-in!  [:include? id] true))))

(defn empty-walk [graph focus-id]
  (let [step (Step :to-id focus-id)]
    (transient
     (hash-map :graph     graph
               :focus-id  focus-id
               :steps     (transient {})
               :nodes     (atom (transient {}))
               :include?  (transient {})
               :ids       (transient [])
               :count     0
               :to-follow (queue step)))))

(defn persist-walk! [walk]
  (persistent!
   (-> walk
       (update-in! [:steps] persistent!)
       (update-in! [:nodes] swap! persistent!)
       (update-in! [:include?] persistent!)
       (update-in! [:ids] persistent!))))

(defn assoc-step [walk step]
  (if (or (back? step) (walked? walk step))
    walk
    (let [to-id (step :to-id)]
      (-> walk
          (add-node to-id)
          (assoc-in!  [:steps to-id (step :from-id) (step :layer)] step)
          (update-in! [:to-follow] conj step)))))

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
          walk (update-in! walk [:to-follow] pop)]
      (if (or (nil? step) (< limit (walk :count)))
        (persist-walk! walk)
        (recur (follow walk :tree step))))))