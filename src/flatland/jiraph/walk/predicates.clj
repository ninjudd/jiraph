(ns flatland.jiraph.walk.predicates
  (:use [flatland.useful.utils :only [defm]])
  (:use [flatland.jiraph.walk :only [result-count from-id distance]]))

(defn at-limit
  "Returns a function that can be passed as the :terminate? traversal parameter
  to limit a walk to a specific number of steps."
  [num]
  (fn [walk]
    (<= num (result-count walk))))

(defn has-distance
  "Return a predicate taking [walk step], which applies the supplied test to the
   step's distance. The distance of a step is the distance from the walk's
   initial node to the step's destination node."
  [test num]
  (fn [walk step]
    (test (distance step) num)))

(defn initial-step?
  "Is this the first step of the walk? Two arity verson can be used for :traverse?, :add?
  and :follow?."
  ([_ step] (initial-step? step))
  ([step]   (nil? (from-id step))))
