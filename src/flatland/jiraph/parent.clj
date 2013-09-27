(ns flatland.jiraph.parent
  (:use flatland.jiraph.wrapped-layer)
  (:require [flatland.jiraph.layer :as layer :refer [child children]]
            [flatland.jiraph.graph :as graph :refer [update-in-node]]
            [flatland.retro.core :as retro :refer [at-revision current-revision]]))

(defwrapped ParentLayer
  [layer child-map]
  [layer (vals child-map)]

  layer/Parent
  (children [this]
    (reduce into #{}
            [(keys child-map)
             (children layer)]))
  (child [this child-name]
    (if-let [child (get child-map child-name)]
      (at-revision child (current-revision this))
      (child (at-revision layer (current-revision this)) child-name))))

(defn make [layer child-map]
  (ParentLayer. layer child-map))