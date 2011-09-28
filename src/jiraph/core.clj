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

(defmacro with-each-layer
  "Execute forms with layer bound to each layer specified or all layers if layers is empty."
  [layers & forms]
  `(let [layers# ~layers]
     (doseq [[~'layer-name ~'layer] (cond (keyword? layers#) [layers# (layer layers#)]
                                          (empty?   layers#) *graph*
                                          :else              (select-keys *graph* layers#))]
       (when *verbose*
         (println (format "%-20s %s"
                          ~'layer-name
                          (s/join " " (map pr-str '~forms)))))
       ~@forms)))

(defn open! []
  (with-each-layer []
    (layer/open layer)))

(defn close! []
  (with-each-layer []
    (layer/close layer)))

(defn set-graph! [graph]
  (alter-var-root #'*graph* (fn [_] graph)))

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
  `(if (read-only?)
     (do ~@forms)
     ((reduce
       retro/wrap-transaction
       (fn [] ~@forms)
       (cond (keyword? ~layers) [(layer ~layers)]
             (empty?   ~layers) (vals *graph*)
             :else              (map layer ~layers))))))