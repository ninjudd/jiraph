(ns jiraph.walk
  (:use [useful :only [assoc-in! update-in! queue conj-vec update construct into-map]])
  (:require [jiraph.graph :as graph]))

(defprotocol Walk "Jiraph walk protocol"
  (traverse?     [walk step])
  (follow?       [walk step])
  (add?          [walk step])
  (follow-layers [walk step])
  (alias-ids     [walk layer step])
  (init-step     [walk step])
  (reduce-step   [walk from-step to-step]))

(defrecord Step [id from-id layer source edge])

(def default-walk-impl
  {:traverse?     (fn [walk step] true)
   :follow?       (fn [walk step] true)
   :add?          (fn [walk step] true)
   :follow-layers (fn [walk step] (graph/layers))
   :alias-ids     (fn [walk layer step] [(:id step)])
   :init-step     (fn [walk step] step)
   :reduce-step   (fn [walk from-step to-step] to-step)})

(defn walk-fn [[name f]]
  [name (if (fn? f) f (fn [& _] f))])

(defmacro defwalk [name & methods]
  `(do (defrecord ~name ~'[focus-id steps nodes include? ids count to-follow sort-edges])
       (extend ~name
         Walk (into default-walk-impl (map walk-fn (into-map ~@methods))))))

(defn lookup-node [walk layer id]
  (or (@(:nodes walk) [id layer])
      (let [node (graph/get-node layer id)]
        (swap! (:nodes walk) assoc-in! [[id layer]] node)
        node)))

(defn walked? [walk step]
  (some #(and (= (:from-id step) (:from-id %)) (= (:layer step) (:layer %)))
        (get-in walk [:steps (:id step)])))

(defn back? [step]
  (= (:id step)
     (get-in step [:source :from-id])))

(defn initial? [step]
  (nil? (:from-id step)))

(defn- add-node [walk step]
  (let [id (:id step)]
    (if (or ((:include? walk) id) (not (add? walk step)))
      walk
      (-> walk
          (update-in [:include?] conj! id)
          (update-in [:ids]      conj! id)
          (update-in [:count]    inc)))))

(defn- persist-walk! [walk]
  (swap! (:nodes walk) persistent!)
  (-> walk
      (update-in [:include?] persistent!)
      (update-in [:ids]      persistent!)
      (update-in [:steps]    persistent!)))

(defn conj-step [walk step]
  (if (or (back? step) (walked? walk step) (not (traverse? walk step)))
    walk
    (-> walk
        (add-node step)
        (update-in! [:steps (:id step)] conj-vec step)
        (update-in  [:to-follow]        conj step))))

(defn- make-step [walk from-step layer [to-id edge]]
  (let [from-id (:id from-step)
        to-step (Step. to-id from-id layer from-step edge)]
    (reduce-step walk from-step to-step)))

(defn- layer-steps [walk step layer]
  (let [nodes (map (partial lookup-node walk layer) (alias-ids walk layer step))]
    (map (partial make-step walk step layer) ((:sort-edges walk) (mapcat :edges nodes)))))

(defn follow [walk step]
  (if (follow? walk step)
    (reduce conj-step walk
      (mapcat (partial layer-steps walk step)
              (follow-layers walk step)))
    walk))

(defn init-walk [type focus-id opts]
  (let [sort (or (:sort-edges opts) identity)
        walk (construct type focus-id (transient {}) (atom (transient {})) (transient #{}) (transient []) 0 (queue) sort)
        step (init-step walk (Step. focus-id nil nil nil nil))]
    (conj-step walk step)))

(defn walk [type focus-id & opts]
  (let [opts  (apply hash-map opts)
        limit (opts :limit)]
    (loop [walk (init-walk type focus-id opts)]
      (let [step (-> walk :to-follow first)
            walk (update-in! walk [:to-follow] pop)]
        (if (or (nil? step) (and limit (< limit (:count walk))))
          (persist-walk! walk)
          (recur (follow walk step)))))))

(defn- make-path [step]
  (loop [step step, path ()]
    (if step
      (recur (:source step) (conj path step))
      path)))

(defn paths [walk id]
  (map make-path (get-in walk [:steps id])))

(defn path [walk id]
  (make-path (first (get-in walk [:steps id]))))