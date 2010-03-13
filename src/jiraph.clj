(ns jiraph
  (:use jiraph.utils)
  (:use clojure.contrib.java-utils)
  (:use protobuf)
  (:use jiraph.tc)
  (:import java.io.File))

(defn- callback-name [when what]
  (keyword (str (name when) "-" (name what))))

(defn- default-callback [opts default actions]
  (let [pre-default  (opts (callback-name :pre  default))
        post-default (opts (callback-name :post default))]
    (reduce
     (fn [opts action]
       (-> opts
           (assoc-or (callback-name :pre  action) pre-default)
           (assoc-or (callback-name :post action) post-default)))
     opts actions)))

(defmacro defgraph [sym & args]
  (let [layer? #(and (list? %) (= 'layer (first %)))
        layers (filter layer? args)
        opts   (eval (apply hash-map (remove layer? args)))
        open-layer
        (fn [graph [_ layer & args]]
          (let [opts  (merge opts (apply hash-map args))
                proto (if (opts :proto) (protodef (eval (opts :proto))))]
            (assoc graph layer
              (db-open (-> opts
                           (assoc :proto proto)
                           (assoc :path (str (opts :path) "/" (name layer)))
                           (default-callback :write [:add :append :update :delete])
                           (default-callback :alter [:add :append :update])
                           (assoc-if proto
                             :dump protobuf-dump
                             :load (partial protobuf-load proto)))))))]
    (when (opts :create) (.mkdir (File. #^String (opts :path))))
    `(def ~sym ~(reduce open-layer {} layers))))

(defn close-graph [graph]
  (map db-close (vals graph)))

(declare *graph*)
(declare *callback*)

(defmacro with-graph [graph & body]
  `(binding [*graph* ~graph]
     (do ~@body)))

(defn- opt [layer key]
  ((*graph* layer) key))

(defn- callback [name layer args]
  (if-let [method (opt layer name)]
    (binding [*callback* name]
      (apply method layer args)
      true)))

(defmacro with-callbacks [action args & body]
  (let [layer (first args)
        args  (subvec args 1)
        node  (peek (subvec args 1))
        node? (not (nil? node))
        pre   (callback-name :pre  action)
        post  (callback-name :post action)
        val   `val# ; Generate val symbol so it can be used in all syntax-quotes.
        body  `(when-let [ret# (do ~@body)]
                 (callback ~post ~layer ~args)
                 ret#)]
    `(let [~val (callback ~pre ~layer ~args)]
       (when-not (false? ~val)
         ~(if node?
            `(let [~node (if (associative? ~val) ~val ~node)]
               ~body)
            body)))))

(defmacro transaction [layer & body]
  `(db-transaction (*graph* ~layer) ~@body))

(defn make-node [layer & args]
  (if-let [proto (opt layer :proto)]
    (apply protobuf proto args)
    (apply hash-map args)))

(defn get-node
  ([layer id]     (db-get (*graph* layer) id))
  ([layer id len] (db-get-slice (*graph* layer) id len)))

(defn add-node! [layer id & args]
  {:pre [(not (opt layer :append-only))]}
  (let [node (apply make-node layer :id id args)]
    (with-callbacks :add [layer id node]
      (db-add (*graph* layer) id node))))

(defn update-node! [layer id update-fn & args]
  {:pre [(not (opt layer :append-only))]}
  (transaction layer
    (let [old (get-node layer id)
          new (apply update-fn old args)]
      (with-callbacks :update [layer id old new]
        (when-not (= old new)
          (db-set (*graph* layer) id new))))))

(defn append-node! [layer id & args]
  {:pre [(not (opt layer :disable-append))]}
  (let [node (apply make-node layer args)]
    (with-callbacks :append [layer id node]
      (if (opt layer :store-length-on-append)
        (transaction layer
          (let [node (assoc node :_len (db-len (*graph* layer) id))]
            (db-append (*graph* layer) id node)))
        (db-append (*graph* layer) id node)))))

(defn delete-node! [layer id]
  (with-callbacks :delete [layer id]
    (db-delete (*graph* layer) id)))

(def conj-node! append-node!)

(defn assoc-node! [layer id & args]
  (update-node! layer id
    (fn [node]
      (if node
        (apply assoc node args)
        (apply make-node layer :id id args)))))

(defn conj-edge! [layer from-id to-id & args]
  (let [edge (apply hash-map :to-id to-id args)]
    (conj-node! layer from-id :edges [edge])
    edge))

(defn get-edges
  ([layer id]     (get-edges (get-node layer id)))
  ([layer id len] (get-edges (get-node layer id len)))
  ([node]
     (persistent!
      (reduce
       (fn [edges edge]
         (let [to-id    (:to-id edge)
               existing (edges to-id)]
           (assoc! edges
             to-id (if existing
                     (merge-with append existing edge)
                     edge))))
       (transient {}) (:edges node)))))

(defn update-edges! [layer id update-fn & args]
  (update-node! layer id
    (fn [node]
      (if node
        (assoc node
          :edges (vec (vals
                  (apply update-fn (get-edges node) args))))
        (let [edges (apply update-fn {} args)]
          (when-not (empty? edges)
            (make-node layer :id id :edges (vec (vals edges)))))))))

(defn update-edge! [layer from-id to-id update-fn & args]
  (update-edges! layer from-id
    (fn [edges]
      (let [edge (apply update-fn (edges to-id) args)]
        (when edge
          (assoc edges
            to-id (assoc edge :to-id to-id)))))))

(defn assoc-edge! [layer from-id to-id & args]
  (apply update-edge! layer from-id to-id assoc args))

(defn delete-edge! [layer from-id to-id]
  (update-edges! layer from-id
    (fn [edges]
      (when (edges to-id)
        (dissoc edges to-id)))))