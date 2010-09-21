(ns jiraph.walk
  (:use jiraph
        [useful :only [assoc-in! update-in!]]))

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

(defmacro defwalkfn [fn-name args default]
  (let [key (keyword (name fn-name))]
    `(defn ~fn-name ~args
       (if-let [f# (~(first args) ~key)]
         (if (fn? f#) (f# ~@args) f#)
         ~default))))

(defwalkfn traverse? [walk step] true)
(defwalkfn follow?   [walk step] true)
(defwalkfn add?      [walk step] true)

(defwalkfn layers [walk step]
  (graph-layers))

(defwalkfn node-ids [walk layer step]
  [(step :id)])

(defwalkfn init-step [walk step]
  step)

(defwalkfn reduce-step [walk from-step to-step]
  to-step)

(defn- add-node [walk step]
  (let [id (step :id)]
    (if (or (get-in walk [:include? id]) (not (add? walk step)))
      walk
      (-> walk
          (update-in! [:ids] conj! id)
          (update-in! [:count] inc)
          (assoc-in!  [:include? id] true)))))

(defn- empty-walk [focus-id opts]
  (let [step (init-step opts (Step :id focus-id))]
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

(defn- persist-walk! [walk]
  (swap! (walk :nodes) persistent!)
  (persistent!
   (-> walk
       (update-in! [:steps] persistent!)
       (update-in! [:include?] persistent!)
       (update-in! [:ids] persistent!))))

(defn assoc-step [walk step]
  (if (or (back? step) (walked? walk step) (not (traverse? walk step)))
    walk
    (-> walk
        (add-node step)
        (update-in! [:steps (step :id)] conj-vec step)
        (update-in! [:to-follow] conj step))))

(defn- make-step [walk from-step layer edge]
  (let [from-id (from-step :id)
        to-id   (edge :to-id)
        to-step (Step [from-step from-id to-id layer edge])]
    (reduce-step walk from-step to-step)))

(defn- sorted-edges [walk nodes]
  (let [edges (mapcat (comp vals :edges) nodes)]
    (if-let [cmp (walk :sort-edges)]
      (sort cmp edges)
      edges)))

(defn- layer-steps [walk step layer]
  (let [nodes (map (partial lookup-node walk layer) (node-ids walk layer step))]
    (map (partial make-step walk step layer) (sorted-edges walk nodes))))

(defn follow [walk step]
  (if (follow? walk step)
    (reduce assoc-step walk
      (mapcat (partial layer-steps walk step)
              (layers walk step)))
    walk))

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