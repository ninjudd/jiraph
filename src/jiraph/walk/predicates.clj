(ns jiraph.walk.predicates
  (:use [useful.utils :only [defm]])
  (:use [jiraph.walk :only [result-count from-id distance]]))

(defm at-limit
  "Returns a function that can be passed as the :terminate? traversal parameter
  to limit a walk to a specific number of steps."
  [num]
  (fn [walk]
    (<= num (result-count walk))))

(defm has-distance
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
