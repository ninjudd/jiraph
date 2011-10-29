(ns jiraph.walk
  (:use [useful.map :only [assoc-in! update-in! update into-map]]
        [useful.utils :only [or-max]]
        [useful.parallel :only [pcollect *pcollect-thread-num*]]
        [useful.java :only [construct]]
        [useful.datatypes :only [make-record assoc-record update-record record-accessors]]
        [fogus.unk :only [memo-lru]]
        useful.debug)
  (:require [jiraph.graph :as graph]
            [clojure.set :as set]))

(def ^{:doc "Should steps be followed in parallel?"} *parallel-follow* false)

(defrecord Step      [id distance from-id layer source edge alt-ids rev data])
(defrecord Walk      [focus-id steps id-set ids result-count to-follow max-rev terminated? traversal])
(defrecord Traversal [traverse? skip? add? follow? count? follow-layers init-step update-step extract-edges terminate?])

(record-accessors Step Walk)

(def default-traversal
  {:traverse?     true
   :skip?         false
   :follow?       true
   :add?          true
   :count?        true
   :follow-layers (fn [walk step] (graph/layers))
   :init-step     (fn [walk step] step)
   :update-step   (fn [walk step] step)
   :extract-edges (fn [walk nodes] (sort-by first (mapcat graph/edges nodes)))
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
     :follow?       [walk step]  Should the edges on this step's node be followed?
     :add?          [walk step]  Should this step's node be added to the walk results?
     :count?        [walk step]  Should this step's node be counted toward the limit after it is added?
     :follow-layers [walk step]  Returns the list of graph layers that should be followed for this step.
     :init-step     [walk step]  Initialize a new step after it is created.
     :update-step   [walk step]  Update the current step before traversing it.
     :extract-edges [walk nodes] Extract a sequence of edges from a group of nodes.
     :terminate?    [walk]       Should the walk terminate (even if there are still unfollowed steps)?

   In, addition if you wish to cache walk results, use the option :cache true."
  [name & defaults]
  `(let [defaults# (into-map default-traversal ~@defaults)]
     (defn ~name {:defaults defaults#} [focus-id# & opts#]
       (let [opts#  (into-map defaults# opts#)
             cache# (:cache opts#)]
         ((if cache# walk cached-walk)
          focus-id#
          (into (make-record Traversal)
                (map traversal-fn (dissoc opts# :cache))))))))

(defn- walked?
  "Has this step already been traversed?"
  [walk step]
  (some (fn [s] (and (= (from-id step) (from-id s))
                     (= (layer   step) (layer   s))))
        (get (steps walk) (id step))))

(defn- back?
  "Is this step back across the edge that was just crossed on the last step?"
  [step]
  (= (id step)
     (when-let [source (source step)]
       (from-id source))))

(defn includes? [walk id]
  (contains? (id-set walk) id))

(defn- add-node
  "Add the node associated with this step to the walk results."
  [^Walk walk step]
  (let [id (id step)]
    (if (or ((id-set walk) id)  ;; this can not use the includes? helper fn because transient sets to do not work with contains?
            (not (<< add? walk step)))
      walk
      (update-record walk
        (conj! ids id)
        (conj! id-set id)
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
                (update-in! steps [(id step)] (fnil conj []) step)
                (or-max max-rev (:rev step)))
              (add-node step)))))))

(defn- make-step
  "Create a new step from the previous step, layer and edge."
  [walk from-step layer rev [to-id edge]]
  (<< init-step walk
      (make-record Step
        :id to-id
        :distance (inc (distance from-step))
        :from-id (id from-step)
        :layer layer
        :source from-step
        :edge edge
        :rev rev)))

(defn- make-layer-steps
  "Create steps for all outgoing edges on this layer for this step's node(s)."
  [walk step layer]
  (let [ids   (or (alt-ids step) [(id step)])
        nodes (map #(graph/get-node layer %) ids)
        rev   (apply or-max (map :rev nodes))]
    (map #(make-step walk step layer rev %)
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
               :focus-id     focus-id
               :steps        (transient {})
               :id-set       (transient #{})
               :ids          (transient [])
               :result-count 0
               :to-follow    (transient [])
               :traversal    traversal)
        step (<< init-step walk (make-record Step :id focus-id :distance 0))]
    (traverse walk step)))

(defn- persist-walk!
  "Transform a transient walk into a persistent walk once the walk is complete."
  [^Walk walk]
  (update-record walk
    (persistent! id-set)
    (persistent! steps)
    (persistent! ids)
    (persistent! to-follow)))

(defn walk
  "Perform a walk starting at focus-id using traversal which should be of type jiraph.walk.Traversal."
  [focus-id traversal]
  (let [map (if *parallel-follow*
              (partial pcollect graph/wrap-bindings)
              map)]
    (graph/with-caching
      (loop [^Walk walk (init-walk traversal focus-id)]
        (let [steps (persistent! (to-follow walk))
              walk  (assoc-record walk :to-follow (transient []))]
          (if (empty? steps)
            (persist-walk! walk)
            (recur
             (reduce traverse walk
                     (apply concat (map (partial follow walk) steps))))))))))

(def cached-walk (? (memo-lru walk 5)))

(defn make-path
  "Given a step, construct a path of steps from the walk focus to this step's node."
  [step]
  (when step
    (loop [^Step step step, path nil]
      (if step
        (recur (source step) (conj path (assoc-record step :source nil)))
        (when path (vec path))))))

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
  (seq (remove nil? (map (:id-set walk2) (:ids walk1)))))