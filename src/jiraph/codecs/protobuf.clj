(ns jiraph.codecs.protobuf
  (:use [jiraph.codecs :only [revisioned]]
        [useful.map :only [adjoin]])
  (:require [protobuf.codec :as protobuf]))

(defn protobuf-codec [proto reduce-fn]
  (let [optimized? (= adjoin reduce-fn)
        full       (protobuf/protobuf-codec proto)
        revisioned (-> (protobuf/protobuf-codec proto :repeated true)
                       (revisioned-codec reduce-fn))]
    (fn [{:keys [revision]}]
      (if (and optimized? (nil? revision))
        full
        (revisioned revision)))))
