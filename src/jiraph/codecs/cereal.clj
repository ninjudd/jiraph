(ns jiraph.codecs.cereal
  (:use [jiraph.codecs :only [revisioned-codec]])
  (:require [cereal.core :as cereal]))

(letfn [(cereal-codec [codec] ;; Codec -> (a -> a) -> (opts -> Codec)
          (fn [reduce-fn]
            (-> (constantly (codec :repeated true))
                (revisioned-codec reduce-fn))))]

  (def java-codec    (cereal-codec cereal/java-codec)) ;; (a -> a) -> (opts -> Codec)
  (def clojure-codec (cereal-codec cereal/clojure-codec)))
