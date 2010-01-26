(ns jiraph.utils
  (:use clojure.contrib.math))

(defn assoc-or [map key value]
  (if (map key)
    map
    (assoc map key value)))

(defmacro verify [x exception & body]
  `(if ~x
     (do ~@body)
     (throw (if (string? ~exception)
              (Exception. ~exception)
              ~exception))))

(defn find-index [pred vec]
  (let [n (count vec)]
    (loop [i 0]
      (when-not (= n i)
        (if (pred (nth vec i))
          i
          (recur (inc i)))))))

(defn remove-nth [vec index]
  (concat (subvec vec 0 index) (subvec vec (inc index) (count vec))))

(defmacro let-if [test then-bindings else-bindings & body]
  `(if ~test
     (let ~then-bindings
       ~@body)
     (let ~else-bindings
       ~@body)))

(defmacro defclass [class & fields]
  `(let [type#   (keyword (name (quote ~class)))
         struct# (create-struct ~@fields)]
     (defn ~class [& args#]
       (let [instance# (if (= (count args#) 1)
                         (let [attrs# (first args#)]
                           (cond (map? attrs#)    (merge (struct-map struct#) attrs#)
                                 (vector? attrs#) (apply struct struct# attrs#)
                                 (seq?    attrs#) (apply struct-map struct# attrs#)
                                 :else (throw (IllegalArgumentException. "single arg must be map or vector"))))
                         (apply struct-map struct# args#))]
         (with-meta instance# {:type type#})))))

(defn take-rand [vec]
  (let [i (rand-int (count vec))]
    (vec i)))

(defn queue
  ([]    clojure.lang.PersistentQueue/EMPTY)
  ([seq] (if (sequential? seq)
           (into (queue) seq)
           (conj (queue) seq))))

(defn slice [n coll]
  (loop [num    n
         slices []
         items  (vec coll)]
    (if (empty? items)
      slices
      (let [size (ceil (/ (count items) num))]
        (recur (dec num) (conj slices (subvec items 0 size)) (subvec items size))))))

(defn pcollect [f coll]
  (let [n    (.. Runtime getRuntime availableProcessors)
        rets (map #(future (map f %)) (slice n coll))]
    (mapcat #(deref %) rets)))