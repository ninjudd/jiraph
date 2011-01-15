(ns jiraph.walk
  (:use [useful :only [assoc-in! update-in! queue conj-vec update construct into-map]]
        [useful.string :only [dasherize]])
  (:require [jiraph.graph :as graph]))

(defprotocol Walk "Jiraph walk protocol"
  (traverse?     [walk step] "Should this step be traversed and added to the follow queue?")
  (add?          [walk step] "Should this step's node be added to the walk results?")
  (follow?       [walk step] "Should the edges on this step's node be followed?")
  (follow-layers [walk step] "Returns the list of graph layers that should be followed for this step.")
  (init-step     [walk step] "Initialize the step that starts the walk.")
  (update-step   [walk step] "Update the current step before traversing it based on the walk state."))

(defrecord Step [id from-id layer source edge ids])

(def default-walk-impl
  {:traverse?     (fn [walk step] true)
   :follow?       (fn [walk step] true)
   :add?          (fn [walk step] true)
   :follow-layers (fn [walk step] (graph/layers))
   :init-step     (fn [walk step] step)
   :update-step   (fn [walk step] step)})

(defn walk-fn [[name f]]
  [name (if (fn? f) f (fn [& _] f))])

(defmacro defwalk
  "Define a new walk based on the default-walk-impl given custom method implementations as maps or key/value pairs.
   Creates a type called name that satisfies the Walk protocol. Also creates a walk function called walk-name using
   the dasherized version of name (e.g. type: Collaborators walk-fn: walk-collaborators)."
  [name & methods]
  (let [fn-name (symbol (str "walk-" (dasherize name)))]
    `(do (defrecord ~name ~'[focus-id steps nodes include? ids count to-follow sort-edges])
         (extend ~name
           Walk (into default-walk-impl
                      (map walk-fn (into-map ~@methods))))
         (defn ~fn-name [& args#]
           (apply walk ~name args#)))))

(defn lookup-node
  "Get the specified node and cache it in the walk if it isn't already cached."
  [walk layer id]
  (or (@(:nodes walk) [id layer])
      (let [node (graph/get-node layer id)]
        (swap! (:nodes walk) assoc-in! [[id layer]] node)
        node)))

(defn walked?
  "Has this step already been traversed?"
  [walk step]
  (some #(and (= (:from-id step) (:from-id %)) (= (:layer step) (:layer %)))
        (get-in walk [:steps (:id step)])))

(defn back?
  "Is this step back across the edge that was just crossed on the last step?"
  [step]
  (= (:id step)
     (get-in step [:source :from-id])))

(defn initial?
  "Is this the first step of the walk?"
  [step]
  (nil? (:from-id step)))

(defn- add-node
  "Add the node associated with this step to the walk results."
  [walk step]
  (let [id (:id step)]
    (if (or ((:include? walk) id) (not (add? walk step)))
      walk
      (-> walk
          (update-in [:include?] conj! id)
          (update-in [:ids]      conj! id)
          (update-in [:count]    inc)))))

(defn- traverse
  "Record this step as traversed and add it to the follow queue."
  [walk step]
  (if (or (back? step) (walked? walk step) (not (traverse? walk step)))
    walk
    (-> walk
        (add-node step)
        (update-in! [:steps (:id step)] conj-vec step)
        (update-in  [:to-follow]        conj step))))

(defn- make-step
  "Create a new step from the previous step, layer and edge."
  [walk from-step layer [to-id edge]]
  (let [from-id (:id from-step)
        to-step (Step. to-id from-id layer from-step edge nil)]
    (update-step walk to-step)))

(defn- make-layer-steps
  "Create steps for all outgoing edges on this layer for this step's node(s)."
  [walk step layer]
  (let [ids   (or (:ids step) [(:id step)])
        nodes (map (partial lookup-node walk layer) ids)]
    (map (partial make-step walk step layer)
         ((:sort-edges walk) (mapcat :edges nodes)))))

(defn- follow
  "Create and traverse steps for all outgoing edges on this step."
  [walk step]
  (if (follow? walk step)
    (reduce traverse walk
      (mapcat (partial make-layer-steps walk step)
              (follow-layers walk step)))
    walk))

(defn- init-walk
  "Create an empty walk."
  [type focus-id opts]
  (let [walk (merge (construct type focus-id (transient {}) (atom (transient {}))
                               (transient #{}) (transient []) 0 (queue) identity)
                    opts)
        step (init-step walk (Step. focus-id nil nil nil nil nil))]
    (traverse walk step)))

(defn- persist-walk!
  "Transform a transient walk into a persistent walk once the walk is complete."
  [walk]
  (swap! (:nodes walk) persistent!)
  (-> walk
      (update-in [:include?] persistent!)
      (update-in [:ids]      persistent!)
      (update-in [:steps]    persistent!)))

(defn walk
  "Perform a walk starting at focus-id using the given walk type (which must satisfy jiraph.walk/Walk).
   Supported opts:
     :sort-edges - a fn to sort the outgoing edges for a given layer before traversing them"
  [type focus-id & opts]
  (let [opts  (apply into-map opts)
        limit (opts :limit)]
    (loop [walk (init-walk type focus-id opts)]
      (let [step (-> walk :to-follow first)
            walk (update-in! walk [:to-follow] pop)]
        (if (or (nil? step) (and limit (< limit (:count walk))))
          (persist-walk! walk)
          (recur (follow walk step)))))))

(defn- make-path
  "Given a step, construct a path from the walk focus to this step's node."
  [step]
  (loop [step step, path ()]
    (if step
      (recur (:source step) (conj path step))
      path)))

(defn paths
  "Return all paths to a given node in walk."
  [walk id]
  (map make-path (get-in walk [:steps id])))

(defn path
  "Return the shortest path to a given node in walk."
  [walk id]
  (make-path (first (get-in walk [:steps id]))))