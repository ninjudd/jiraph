(ns jiraph.walk
  (:use jiraph)
  (:use jiraph.utils))

(defclass Step :source :from-id :id :layer :edge)

(defn lookup-node [walk layer id]
  (or (@(walk :nodes) [id layer])
      (let [node (get-node (walk :graph) layer id)]
        (swap! (walk :nodes) assoc! [id layer] node)
        node)))

(defn walked? [walk step]
  (not (nil? (get-in walk [:steps (step :id) (step :from-id) (step :layer)]))))

(defn back? [step]
  (= (step :id)
     (get-in step [:source :from-id])))

(defn- follow? [walk step]
  (if-let [follow? (walk :follow?)]
    (follow? step)
    true))

(defn- add? [walk step]
  (if-let [add? (walk :follow?)]
    (add? step)
    true))

(defn- init-step [step opts]
  (if-let [init (opts :init)]
    (init step)
    step))

(defn add-node [walk step]
  (let [id (step :id)]
    (if (or (get-in walk [:include? id]) (not (add? walk step)))
      walk
      (-> walk
          (update-in! [:ids] conj! id)
          (update-in! [:count] inc)
          (assoc-in!  [:include? id] true)))))

(defn empty-walk [graph focus-id opts]
  (let [step (init-step (Step :id focus-id) opts)]
    (transient
     (assoc opts
            :graph     graph
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
  (if (or (back? step) (walked? walk step) (not (follow? walk step)))
    walk
    (-> walk
        (add-node step)
        (assoc-in!  [:steps (step :id) (step :from-id) (step :layer)] step)
        (update-in! [:to-follow] conj step))))

(defn layers [walk step]
  (let [layers (walk :layers)]
    (if (fn? layers)
      (layers step)
      layers)))

(defn- make-step [walk from-step layer edge]
  (let [from-id (from-step :id)
        to-id   (edge :to-id)
        to-step (Step [from-step from-id to-id layer edge])]
    (if-let [reduce (walk :reduce)]
      (reduce from-step to-step)
      to-step)))

(defn- layer-steps [walk step layer]
  (let [node  (lookup-node walk layer (step :id))
        edges (if node (node :edges) [])]
    (map (partial make-step walk step layer) edges)))

(defn follow [walk step]
  (reduce assoc-step walk
    (mapcat (partial layer-steps walk step)
            (layers walk step))))

(defn walk [graph focus-id & args]
  (let [opts  (args-map args)
        limit (opts :limit)]
    (loop [walk (empty-walk graph focus-id opts)]
      (let [step (-> walk :to-follow first)
            walk (update-in! walk [:to-follow] pop)]
        (if (or (nil? step) (and limit (< limit (walk :count))))
          (persist-walk! walk)
          (recur (follow walk step)))))))


;; (walk g "profile:1" :limit 100
;;       :follow? (fn [step])
;;       :add?    (fn [step])
;;       :layers  (fn [step])
;;       :reduce  (fn [prev-step step])
;; )