(ns jiraph.core
  (:require [jiraph.graph :as graph]
            [clojure.string :as s]))

(def ^{:dynamic true} *graph* nil)
(def ^{:dynamic true} *verbose* nil)
(def ^{:dynamic true} *use-outer-cache* nil)

(defn layer
  "Return the layer for a given name from *graph*."
  [layer-name]
  (if *graph*
    (or (get *graph* layer-name)
        (throw (java.io.IOException. (format "cannot find layer %s in open graph" layer-name))))
    (throw (java.io.IOException. (format "attempt to use a layer without an open graph")))))

(defn as-layer-map
  "Create a map of {layer-name, layer} pairs from the input. A keyword yields a
   single-entry map, an empty sequence operates on all layers, and a sequence of
   keywords causes each to be resolved to its actual layer."
  [layers]
  (cond (keyword? layers) {layers (layer layers)}
        (empty? layers)   *graph*
        :else             (select-keys *graph* layers)))

(defmacro with-each-layer
  "Execute forms with layer bound to each layer specified or all layers if layers is empty."
  [layers & forms]
  `(doseq [[~'layer-name ~'layer] (as-layer-map ~layers)]
     (when *verbose*
       (println (format "%-20s %s"
                        ~'layer-name
                        (s/join " " (map pr-str '~forms)))))
     ~@forms))

(defn open! []
  (with-each-layer []
    (layer/open layer)))

(defn close! []
  (with-each-layer []
    (layer/close layer)))

(defn set-graph! [graph]
  (alter-var-root #'*graph* (constantly graph)))

(defmacro with-graph [graph & forms]
  `(binding [*graph* ~graph]
     (try (open!)
          ~@forms
          (finally (close!)))))

(defmacro with-graph! [graph & forms]
  `(let [graph# *graph*]
     (set-graph! ~graph)
     (try (open!)
          ~@forms
          (finally (close!)
                   (set-graph! graph#)))))

(defmacro with-transaction
  "Execute forms within a transaction on the named layer/layers."
  [layers & forms]
  `(graph/with-transaction (vals (as-layer-map ~layers))
     ~@forms))

(defn current-revision
  "The maximum revision on all specified layers, or all layers if none are specified."
  [& layers]
  (apply max 0 (for [layer (vals (as-layer-map layers))]
                 (or (graph/get-property layer :rev) 0))))