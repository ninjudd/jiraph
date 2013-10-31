(ns flatland.jiraph.core
  (:use [flatland.useful.utils :only [returning memoize-deref map-entry adjoin invoke]]
        [flatland.useful.map :only [update into-map]]
        [flatland.useful.macro :only [macro-do]]
        [flatland.useful.fn :only [fix !]]
        slingshot.slingshot)
  (:require [flatland.jiraph.graph :as graph]
            [flatland.jiraph.layer :as layer]
            [clojure.string :as s]
            [flatland.retro.core :as retro])
  (:import java.io.IOException))

(def ^{:dynamic true} *graph*    nil)
(def ^{:dynamic true} *revision* nil)

(defn get-layer [layer-spec]
  (let [layer-spec (fix layer-spec (! vector?) vector)]
    (when-let [layer (reduce (fn [layer child-name]
                               (when layer
                                 (graph/child layer child-name)))
                             (get *graph* (first layer-spec))
                             (rest layer-spec))]
      (retro/at-revision layer *revision*))))

(defn layer
  "Return the layer for a given name from *graph*."
  [layer-spec]
  (if *graph*
    (get-layer layer-spec)
    (throw (IOException. (format "attempt to use a layer without an open graph")))))

(defn layer-entries
  ([] *graph*)
  ([id] (for [[name layer] *graph*
              :when (seq (graph/schema layer id))]
          (map-entry name (retro/at-revision layer *revision*)))))

(defn layer-names
  "Return the names of all layers in the current graph."
  ([]   (keys *graph*))
  ([id] (map key (layer-entries id))))

(defn layers
  "Return all layers in the current graph."
  ([]   (map layer (layer-names)))
  ([id] (map val (layer-entries id))))

(defn as-layer-map
  "Create a map of {layer-name, layer} pairs from the input. A keyword yields a
   single-entry map, an empty sequence operates on all layers, and a sequence of
   keywords causes each to be resolved to its actual layer."
  [layers]
  (if (keyword? layers)
    {layers (layer layers)}
    (let [names (if (empty? layers)
                  (keys *graph*)
                  (filter (set layers) (keys *graph*)))]
      (into {} (for [name names]
                 [name (layer name)])))))

(defmacro dotxn [layer-spec & forms]
  `(graph/dotxn [(layer ~layer-spec)]
     (do ~@forms)))

(macro-do [fname]
  (let [impl (symbol "flatland.jiraph.graph" (name fname))]
    `(defmacro ~fname ~'[actions]
       (list '~impl ~'actions)))
  txn* txn
  unsafe-txn* unsafe-txn)

(defmacro txn-> [layer-spec & forms]
  (let [name (gensym 'name)]
    `(let [~name ~layer-spec]
       (do (graph/txn (graph/compose ~@(for [form forms]
                                         `(-> ~name ~form))))
           ~name))))

(letfn [(symbol [& args]
          (apply clojure.core/symbol (map name args)))]
  (defn- graph-impl [name]
    (let [impl-name (symbol 'flatland.jiraph.graph name)
          var (resolve impl-name)
          meta (-> (meta var)
                   (select-keys [:arglists :doc :macro :dynamic]))]
          {:varname impl-name
           :var var
           :meta meta ;; for use by functions
           :func @var})))

(defn- fix-meta [meta]
  (update meta :arglists (partial list 'quote)))

;; define forwarders to resolve keyword layer-spec in *graph*
(macro-do [name]
  (let [{:keys [varname meta]} (graph-impl name)]
    `(def ~(with-meta name (fix-meta meta))
       (fn ~name [layer-spec# & args#]
         (if-let [layer# (layer layer-spec#)]
           (apply ~varname layer# args#)
           (throw (IOException.
                   (format "cannot find layer %s in open graph" layer-spec#)))))))
  update-in-node  update-node  dissoc-node  assoc-node  assoc-in-node
  update-in-node! update-node! dissoc-node! assoc-node! assoc-in-node!
  node-valid? verify-node)

(macro-do [name]
  (let [{:keys [varname meta]} (graph-impl name)]
    `(def ~(with-meta name (fix-meta meta))
       (fn ~name [layer-spec# & args#]
         (when-let [layer# (layer layer-spec#)]
           (apply ~varname layer# args#)))))
  get-node find-node query-in-node get-in-node get-edges get-edge-ids get-edge
  get-revisions node-history get-incoming get-incoming-map
  node-seq node-rseq node-subseq node-rsubseq node-id-seq node-id-rseq
  node-id-subseq node-id-rsubseq fields)

;; these point directly at flatland.jiraph.graph functions, without layer-spec resolution
;; or any indirection, because they can't meaningfully work with layer names but
;; we don't want to make the "simple" uses of flatland.jiraph.core have to mention
;; flatland.jiraph.graph at all
(doseq [name '[wrap-caching with-caching]]
  (let [{:keys [func meta]} (graph-impl name)]
    (intern *ns* (with-meta name meta) func)))

;; operations on a list of layers
(macro-do [name]
  (let [{:keys [varname meta]} (graph-impl name)]
    `(def ~(with-meta name (fix-meta meta))
       (fn ~name [& layers#]
         (apply ~varname (vals (as-layer-map layers#))))))
  open close touch sync! optimize! truncate!)

(defmacro at-revision
  "Execute the given forms with the current revision set to rev. Can be used to mark changes with a
   given revision, or read the state at a given revision."
  [rev & forms]
  `(binding [*revision* ~rev]
      ~@forms))

(defn set-graph! [graph]
  (alter-var-root #'*graph* (constantly graph)))

(defmacro with-graph [graph & forms]
  `(let [graph# ~graph]
     (binding [*graph* graph#]
       (try (open)
            ~@forms
            (finally (close))))))

(defmacro with-graph! [graph & forms]
  `(let [graph# *graph*]
     (set-graph! ~graph)
     (try (open)
          ~@forms
          (finally (close)
                   (set-graph! graph#)))))

(letfn [(all-revisions [combine layers]
          (apply combine
                 (or (seq (mapcat retro/revision-range
                                  (vals (as-layer-map layers))))
                     [0])))]
  (defn current-revision
    "The minimum revision on all specified layers, or all layers if none are specified."
    [& layers]
    (all-revisions min layers))
  (defn uncommitted-revision
    "The maximum revision on all specified layers, or all layers if none are specified."
    [& layers]
    (all-revisions max layers)))

(defn layer-exists?
  "Does the named layer exist in the current graph?"
  [layer-spec]
  (not (nil? (get-layer layer-spec))))

(defn schema-by-layer
  "Get the schema for an id across all layers, indexed by layer.

   Optionally you may pass in a graph to use instead of *graph*, to allow
   things like filtering which layers are included in the schema."
  ([id] (schema-by-layer id *graph*))
  ([id graph]
     (into {}
           (for [[layer-name layer] graph
                 :let [schema (graph/schema layer id)]
                 :when (seq schema)]
             [layer-name (:fields schema)]))))

(defn schema-by-attr
  "Get the schema for an id across all layers, indexed by attribute name.

   Optionally you may pass in a graph to use instead of *graph*, to allow
   things like filtering which layers are included in the schema."
  ([id] (schema-by-attr id *graph*))
  ([id graph]
     (apply merge-with conj {}
            (for [[layer-name attrs] (schema-by-layer id graph)
                  [attr field-schema] attrs]
              {attr {layer-name field-schema}}))))
