(ns jiraph.utils
  (:use clojure.contrib.math))

(defn tap [f obj]
  (f obj)
  obj)

(defn args-map
  ([arg & args]
     (args-map (conj args arg)))
  ([args]
     (loop [args args map {}]
       (if (empty? args)
         map
         (let [arg  (first args)
               args (rest args)]
           (cond
            (nil?  arg) (recur args map)
            (map?  arg) (recur args (merge map arg))
            (coll? arg) (recur (into args (reverse arg)) map)
            :else       (recur (rest args) (assoc map arg (first args)))))))))

(defmacro assoc-or [map key value]
  `(if (~map ~key)
     ~map
     (assoc ~map ~key ~value)))

(defmacro assoc-if [map test & args]
  (let [assoc (cons 'assoc (cons map args))]
    `(if ~test
       ~assoc
       ~map)))

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

(defmacro let-if [test bindings & body]
  `(if ~test
     (let ~bindings ~@body)
     (do ~@body)))

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

(defn conj-vec [coll item]
  (if (instance? clojure.lang.PersistentVector coll)
    (conj coll item)
    (conj (vec coll) item)))

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

(defn read-mult [str]
  (let [r (java.io.PushbackReader. (java.io.StringReader. str))]
    (loop [items []]
      (try
       (recur (conj items (read r)))
       (catch Exception _ items)))))

(defn append [left right]
  (cond (map? left)  (merge-with append left right)
        (coll? left) (if (sequential? right) (into left right) (conj left right))
        :else        right))

(defn read-append [str]
  (apply merge-with append (read-mult str)))

(defn read-append-bytes
  ([#^bytes b] (read-append (String. b)))
  ([#^bytes b len] (read-append (String. b 0 (int len)))))

(defn sort-with
  "Sort using decorate-sort-undecorate (Schwartzian transform)."
  ([keyfn coll]
     (sort-with keyfn compare coll))
  ([keyfn #^java.util.Comparator comp coll]
     (let [keyvals (map #(list (keyfn %) %) coll)]
       (map second
            (sort (fn [x y] (. comp (compare (first x) (first y)))) keyvals)))))

(defn any [& preds]
  (fn [& args]
    (some #(apply % args) preds)))

(defn all [& preds]
  (fn [& args]
    (every? #(apply % args) preds)))

(defn assoc-in!
  "Associates a value in a nested associative structure, where ks is a sequence of keys
  and v is the new value and returns a new nested structure. The associative structure
  can have transients in it, but if any levels do not exist, non-transient hash-maps will
  be created."
  [m [k & ks :as keys] v]
  (let [assoc (if (instance? clojure.lang.ITransientCollection m) assoc! assoc)]
    (if ks
      (assoc m k (assoc-in! (get m k) ks v))
      (assoc m k v))))

(defn update-in!
  "'Updates' a value in a nested associative structure, where ks is a sequence of keys and
  f is a function that will take the old value and any supplied args and return the new
  value, and returns a new nested structure. The associative structure can have transients
  in it, but if any levels do not exist, non-transient hash-maps will be created."
  [m [k & ks] f & args]
  (let [assoc (if (instance? clojure.lang.ITransientCollection m) assoc! assoc)]
    (if ks
      (assoc m k (apply update-in! (get m k) ks f args))
      (assoc m k (apply f (get m k) args)))))