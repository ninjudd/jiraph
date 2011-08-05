(ns jiraph.wrapper
  (:use [useful.experimental :only [with-wrapper]])
  (:require [clojure.string :as s]))

;; wrappers for read/write methods, used with defn-wrapping
(def ^{:dynamic true} *read-wrappers* [])
(def ^{:dynamic true} *write-wrappers* [])

(defn fn-name [wrapper]
  (-> wrapper meta :useful.experimental/call-data :fn-name))

(defn simple-logging-wrapper [f]
  (fn [& args]
    (let [name (fn-name *read-wrappers*)
          ret (apply args f)]
      (println (str "Called: (" (s/join " " (map pr-str
                                                 (cons name args)))
                    ")"
                    " => " (pr-str ret))))))

(defmacro with-logging [& body]
  `(with-wrapper #'*read-wrappers* simple-logging-wrapper
     ~@body))

(defn simple-stubbing-wrapper [f]
  (fn [& args]
    (let [name (fn-name *write-wrappers*)]
      (println (str "Stubbed: ("
                    (s/join " " (map pr-str
                                     (cons name args)))
                    ")"))
      nil)))

(defmacro with-stub-writes [& body]
  `(with-wrapper #'*write-wrappers* simple-stubbing-wrapper
     ~@body))
