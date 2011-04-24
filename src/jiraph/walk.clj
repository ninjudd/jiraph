(ns jiraph.walk
  (:use [useful :only [assoc-in! update-in! conj-vec update construct into-map or-max map-reduce pcollect *pcollect-thread-num*]]
        [useful.datatypes :only [make-record assoc-record update-record record-accessors]])
  (:require [jiraph.graph :as graph]
            [clojure.set :as set]))

(def ^{:doc "Should steps be followed in parallel for increased performance?"}
  *parallel-follow* false)

(defrecord Step      [id from-id layer source edge alt-ids rev data])
(defrecord Walk      [focus-id steps node-accessor include? ids result-count to-follow max-rev terminated? traversal])
(defrecord Traversal [traverse? skip? add? follow? count? follow-layers init-step update-step extract-edges terminate?])

(record-accessors Step Walk)

(def default-traversal
  {:traverse?     true
   :skip?         false
   :follow?       true
   :add?          true
   :count?        true
   :follow-layers (fn [walk step]  (graph/layers))
   :init-step     (fn [walk step]  step)
   :update-step   (fn [walk step]  step)
   :extract-edges (fn [walk nodes] (sort-by first (mapcat :edges nodes)))
   :terminate?    false})

(defn traversal-fn [[key val]]
  [key (if (fn? val) val (constantly val))])

(defmacro << [fname walk & args]
  (let [fname (symbol (str "." fname))]
    `(let [^Traversal t# (traversal ~walk)]
       ((~fname t#) ~walk ~@args))))

(defmacro defwalk
  "Define a new walk based on the default-traversal given custom traversal parameters as maps or
   key/value pairs. Creates a walk function with the given name that takes a focus-id and traversal
   parameter overrides. In this way, walks can be further customized at run time.

   Each traversal parameter can be a function or a constant value, which will be turned into a function.
   The default traversal parameters with their function signatures are:
     :traverse?     [walk step]  Should this step be traversed and added to the follow queue?
     :skip?         [walk step]  Should this step be skipped at traversal time? (the opposite of traverse?)
     :follow?       [walk step]  Should this step's node be added to the walk results?
     :add?          [walk step]  Should the edges on this step's node be followed?
     :count?        [walk step]  Should this step's node be counted toward the limit after it is added?
     :follow-layers [walk step]  Returns the list of graph layers that should be followed for this step.
     :init-step     [walk step]  Initialize a new step after it is created.
     :update-step   [walk step]  Update the current step before traversing it.
     :extract-edges [walk nodes] Extract a sequence of edges from a group of nodes.
     :terminate?    [walk]       Should the walk terminate (even if there are still unfollowed steps)?"
  [name & opts]
  `(let [traversal# (into (make-record Traversal)
                          (map traversal-fn (into-map default-traversal ~@opts)))]
     (defn ~name [focus-id# & opts#]
       (walk focus-id#
             (into traversal# (map traversal-fn (into-map opts#)))))))

(defn limit
  "Returns a function that can be passed as the :terminate? traversal parameter to limit a walk to a
   specific number of steps."
  [num]
  (fn [walk]
    (<= num (result-count walk))))

(defn get-node
  "Get the specified node using the memoized function stored in the walk."
  [walk layer id]
  ((node-accessor walk) layer id))

(defn walked?
  "Has this step already been traversed?"
  [walk step]
  (some (fn [s] (and (= (from-id step) (from-id s))
                     (= (layer   step) (layer   s))))
        (get (steps walk) (id step))))

(defn back?
  "Is this step back across the edge that was just crossed on the last step?"
  [step]
  (= (id step)
     (when-let [source (source step)]
       (from-id source))))

(defn initial?
  "Is this the first step of the walk?"
  [step]
  (nil? (from-id step)))

(defn- add-node
  "Add the node associated with this step to the walk results."
  [^Walk walk step]
  (let [id (id step)]
    (if (or ((include? walk) id)
            (not (<< add? walk step)))
      walk
      (update-record walk
        (conj! ids id)
        (conj! include? id)
        (+ result-count (if (<< count? walk step) 1 0))))))

(defn- traverse
  "Record this step as traversed and add it to the follow queue."
  [^Walk walk step]
  (if (<< terminate? walk)
    (assoc-record walk :terminated? true)
    (if (or (back? step)
            (walked? walk step))
      walk
      (let [step (<< update-step walk step)]
        (if (or (<< skip? walk step)
                (not (<< traverse? walk step)))
          walk
          (-> (update-record walk
                (conj! to-follow step)
                (update-in! steps [(id step)] conj-vec step)
                (or-max max-rev (:rev step)))
              (add-node step)))))))

(defn- make-step
  "Create a new step from the previous step, layer and edge."
  [walk from-step layer rev [to-id edge]]
  (<< init-step walk
      (make-record Step
        :id      to-id
        :from-id (id from-step)
        :layer   layer
        :source  from-step
        :edge    edge
        :rev     rev)))

(defn- make-layer-steps
  "Create steps for all outgoing edges on this layer for this step's node(s)."
  [walk step layer]
  (let [ids         (or (alt-ids step) [(id step)])
        [nodes rev] (map-reduce (partial get-node walk layer)
                                #(or-max %1 (:rev %2)) nil
                                ids)]
    (map (partial make-step walk step layer rev)
         (<< extract-edges walk nodes))))

(defn- follow
  "Create steps for all outgoing edges on this step."
  [walk step]
  (when (<< follow? walk step)
    (mapcat (partial make-layer-steps walk step)
            (<< follow-layers walk step))))

(defn- init-walk
  "Create an empty walk."
  [traversal focus-id]
  (let [walk (make-record Walk
               :focus-id      focus-id
               :steps         (transient {})
               :node-accessor (memoize graph/get-node)
               :include?      (transient #{})
               :ids           (transient [])
               :result-count  0
               :to-follow     (transient [])
               :traversal     traversal)
        step (<< init-step walk (make-record Step :id focus-id))]
    (traverse walk step)))

(defn- persist-walk!
  "Transform a transient walk into a persistent walk once the walk is complete."
  [^Walk walk]
  (update-record walk
    (persistent! include?)
    (persistent! steps)
    (persistent! ids)
    (persistent! to-follow)))

(defn walk
  "Perform a walk starting at focus-id using traversal which should be of type jiraph.walk.Traversal."
  [focus-id traversal]
  (let [map (if *parallel-follow*
              (partial pcollect graph/wrap-bindings)
              map)]
    (loop [^Walk walk (init-walk traversal focus-id)]
      (let [steps (persistent! (to-follow walk))
            walk  (assoc-record walk :to-follow (transient []))]
        (if (empty? steps)
          (persist-walk! walk)
          (recur
           (reduce traverse walk
                   (apply concat (map (partial follow walk) steps)))))))))

(defn- make-path
  "Given a step, construct a path of steps from the walk focus to this step's node."
  [step]
  (loop [step step, path nil]
    (if step
      (recur (source step) (conj path step))
      path)))

(defn paths
  "Return all paths to a given node in walk."
  [walk id]
  (map make-path (get-in walk [:steps id])))

(defn path
  "Return the shortest path to a given node in walk."
  [walk id]
  (make-path (first (get-in walk [:steps id]))))

(defn intersection
  "Helper function to return the intersection between the ids of two walks."
  [walk1 walk2]
  (set/intersection (set (:ids walk1))
                    (set (:ids walk2))))
