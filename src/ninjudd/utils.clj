(ns ninjudd.utils)

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