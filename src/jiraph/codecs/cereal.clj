(ns jiraph.codecs.cereal
  (:use [jiraph.codecs :only [revisioned]])
  (:require [cereal.core :as cereal]))

(letfn [(cereal-codec [codec]
          (fn [reduce-fn]
            (-> (codec :repeated true)
                (revisioned-codec reduce-fn))))]

  (def java-codec    (cereal-codec cereal/java-codec))
  (def clojure-codec (cereal-codec cereal/clojure-codec)))
