(ns jiraph.walk
  (:use [useful :only [assoc-in! update-in! queue conj-vec]])
  (:require [jiraph.graph :as graph]))

(defrecord Step [id from-id layer source edge])

(defn lookup-node [walk layer id]
  (or (@(walk :nodes) [id layer])
      (let [node (graph/get-node layer id)]
        (swap! (walk :nodes) assoc-in! [[id layer]] node)
        node)))

(defn walked? [walk step]
  (some #(and (= (:from-id step) (:from-id %)) (= (:layer step) (:layer %)))
        (get-in walk [:steps (:id step)])))

(defn back? [step]
  (= (:id step)
     (get-in step [:source :from-id])))

(defmacro defwalkfn [fn-name args default]
  (let [key (keyword (name fn-name))]
    `(defn- ~fn-name ~args
       (if-let [f# (~(first args) ~key)]
         (if (fn? f#) (f# ~@args) f#)
         ~default))))

(defwalkfn traverse? [walk step] true)
(defwalkfn follow?   [walk step] true)
(defwalkfn add?      [walk step] true)

(defwalkfn layers [walk step]
  (graph/layers))

(defwalkfn node-ids [walk layer step]
  [(:id step)])

(defwalkfn init-step [walk step]
  step)

(defwalkfn reduce-step [walk from-step to-step]
  to-step)

(defn- add-node [walk step]
  (let [id (:id step)]
    (if (or (get-in walk [:include? id]) (not (add? walk step)))
      walk
      (-> walk
          (update-in! [:ids] conj! id)
          (update-in! [:count] inc)
          (assoc-in!  [:include? id] true)))))

(defn- empty-walk [focus-id opts]
  (let [step (init-step opts (Step. focus-id nil nil nil nil))]
    (transient
     (assoc opts
       :focus-id   focus-id
       :steps      (transient {})
       :nodes      (atom (transient {}))
       :include?   (transient {})
       :ids        (transient [])
       :count      0
       :to-follow  (queue [step])
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
        (update-in! [:steps (:id step)] conj-vec step)
        (update-in! [:to-follow] conj step))))

(defn- make-step [walk from-step layer edge]
  (let [from-id (:id from-step)
        to-id   (:to-id edge)
        to-step (Step. to-id from-id layer from-step edge)]
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
  (let [opts  (apply hash-map args)
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
      (recur (:source step) (conj path step))
      path)))

(defn paths [walk id]
  (map make-path walk (get-in walk [:steps id])))

(defn path [walk id]
  (make-path walk (first (get-in walk [:steps id]))))