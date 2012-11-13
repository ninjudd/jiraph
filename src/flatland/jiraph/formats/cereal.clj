(ns flatland.jiraph.formats.cereal
  (:use [flatland.jiraph.formats :only [revisioned-format]])
  (:require [cereal.core :as cereal]))

(letfn [(cereal-format [codec] ;; Codec -> (a -> a) -> (opts -> Codec)
          (fn [reduce-fn]
            (revisioned-format (constantly {:codec (codec :repeated true)
                                            :reduce-fn reduce-fn}))))]

  (def revisioned-java-format    (cereal-format cereal/java-codec)) ;; (a -> a) -> (opts -> Codec)
  (def revisioned-clojure-format (cereal-format cereal/clojure-codec)))
