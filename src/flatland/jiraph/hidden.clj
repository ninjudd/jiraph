(ns flatland.jiraph.hidden
  (:use flatland.jiraph.wrapped-layer)
  (:require [flatland.jiraph.graph :as graph :refer [update-in-node]]
            [flatland.jiraph.layer :as layer]))

(defwrapped HiddenLayer [layer]
  layer/Schema
  (schema [this id] nil))

(defn make
  "Wrap a layer such that it never reports a schema for any node-id.
   This has the effect that code looking for the layers that apply to a given
   node-id will act as if this layer doesn't exist."
  [layer]
  (HiddenLayer. layer))
