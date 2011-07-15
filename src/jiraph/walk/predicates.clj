(ns jiraph.walk.predicates
  (:use [jiraph.walk :only [result-count from-id distance]]))

(defn limit
  "Returns a function that can be passed as the :terminate? traversal parameter
   to limit a walk to a specific number of steps."
  [num]
  (fn [walk]
    (<= num (result-count walk))))

(defn initial?
  "Is this the first step of the walk?"
  [step]
  (nil? (from-id step)))