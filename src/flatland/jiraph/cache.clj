(ns flatland.jiraph.cache
  (:use flatland.jiraph.wrapped-layer
        [flatland.useful.map :only [update-each]]
        [flatland.useful.utils :only [map-entry]])
  (:require [flatland.jiraph.graph :as graph :refer [update-in-node]]
            [flatland.jiraph.layer :as layer]))

(def ^:private nothing (Object.))
(defn ^:private ignore-nothing [f]
  (fn [x & args]
    (if (= x nothing)
      nothing
      (apply f x args))))

(defn invalidate [cache keyseq]
  (if (empty? keyseq)
    nil ;; invalidating the top level, ie everything
    (let [[k & ks] keyseq]
      (if (contains? cache k)
        (update-in cache [k]
                   #(-> %
                        (dissoc :self)
                        (update-in [:children] invalidate ks)))
        nil))))

(defn lookup-path [keyseq]
  (concat (interpose :children keyseq)
          [:self]))

;; TODO
;; - add tests
;; - cache only identity (loses a feature, but is faster and makes more sense)
;;   - don't store anything under :children when :self exists
;; - ship it, baby!

(defn ensure-present [cache layer keyseq query args]
  (let [path (lookup-path keyseq)
        self-data (get-in cache path)]
    (if (contains? self-data [query args])
      cache
      (update-in cache path
                 assoc [query args]
                 (apply graph/query-in-node* layer keyseq nothing (ignore-nothing query) args)))))

(defn get-cached [cache layer keyseq f args]
  (-> cache
      (swap! ensure-present layer keyseq f args)
      (get-in `(~@keyseq :self ~[f args]))))

;; cache is an atom containing a map. the map has keys corresponding to node ids, and
(defwrapped CachedLayer [layer cache cached-children] []
  layer/Basic
  (get-node [this id not-found]
    (let [cached (get-cached cache layer [id] identity nil)]
      (if (= nothing cached)
        not-found
        cached)))
  (update-in-node [this keyseq f args]
    (fn [read]
      (throw (UnsupportedOperationException. "Refusing to write to a read-only cache."))))

  layer/Optimized
  (query-fn [this keyseq not-found f]
    (when (layer/query-fn layer keyseq not-found f)
      (fn [& args]
        (let [cached (get-cached cache layer keyseq f args)]
          (if (= cached nothing)
            (apply f not-found args)
            cached)))))

  layer/Parent
  (child [this child-name]
    (cached-children child-name)))


(defn make
  "Create a cached view of the given layer (and all child layers). Any data requested once will be
  stored and used to fulfill later requests. No expiration policy is in place, so this should only
  be used for short-term caching. Additionally, write-through is not supported, as the cache is not
  invalidated, so this should be used as a read-only view of a layer."
  [layer]
  (CachedLayer. layer (atom {}) (memoize (fn [child]
                                           (make (layer/child layer child))))))

;; returns a new cached layer with an empty cache
(defn reset [layer]
  (make (:layer layer)))
