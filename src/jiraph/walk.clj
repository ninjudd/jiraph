(ns jiraph.walk
  (:use jiraph)
  (:use jiraph.utils))

(defclass Step :source :from-id :id :layer :edge)

(defn lookup-node [walk layer id]
  (or (@(walk :nodes) [id layer])
      (let [node (get-node layer id)]
        (swap! (walk :nodes) assoc-in! [[id layer]] node)
        node)))

(defn walked? [walk step]
  (some #(and (= (step :from-id) (% :from-id)) (= (step :layer) (% :layer)))
        (get-in walk [:steps (step :id)])))

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

(defn empty-walk [focus-id opts]
  (let [step (init-step (Step :id focus-id) opts)]
    (transient
     (assoc opts
       :focus-id   focus-id
       :steps      (transient {})
       :nodes      (atom (transient {}))
       :include?   (transient {})
       :ids        (transient [])
       :count      0
       :to-follow  (queue step)
       :sort-edges (if-let [s (opts :sort-edges-by)]
                     #(compare (s %1) (s %2))
                     (opts :sort-edges))))))

(defn persist-walk! [walk]
  (swap! (walk :nodes) persistent!)
  (persistent!
   (-> walk
       (update-in! [:steps] persistent!)
       (update-in! [:include?] persistent!)
       (update-in! [:ids] persistent!))))

(defn assoc-step [walk step]
  (if (or (back? step) (walked? walk step) (not (follow? walk step)))
    walk
    (-> walk
        (add-node step)
        (update-in! [:steps (step :id)] conj-vec step)
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
    (if-let [reduce-fn (walk :reduce)]
      (reduce-fn from-step to-step)
      to-step)))

(defn- sorted-edges [walk node]
  (if node
    (let [edges (vals (get-edges node))]
      (if-let [comp (walk :sort-edges)]
        (sort comp edges)
        edges))
    ()))

(defn- layer-steps [walk step layer]
  (let [node (lookup-node walk layer (step :id))]
    (map (partial make-step walk step layer) (sorted-edges walk node))))

(defn follow [walk step]
  (reduce assoc-step walk
    (mapcat (partial layer-steps walk step)
            (layers walk step))))

(defn walk [focus-id & args]
  (let [opts  (args-map args)
        limit (opts :limit)]
    (loop [walk (empty-walk focus-id opts)]
      (let [step (-> walk :to-follow first)
            walk (update-in! walk [:to-follow] pop)]
        (if (or (nil? step) (and limit (< limit (walk :count))))
          (persist-walk! walk)
          (recur (follow walk step)))))))

(defn- make-path [walk step]
  (loop [step step, path ()]
    (if step
      (recur (step :source) (conj path step))
      path)))

(defn paths [walk id]
  (map make-path walk (get-in walk [:steps id])))

(defn path [walk id]
  (make-path walk (first (get-in walk [:steps id]))))