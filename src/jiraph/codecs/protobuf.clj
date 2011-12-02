(ns jiraph.codecs.protobuf
  (:use [jiraph.codecs :only [revisioned-codec]]
        [useful.map :only [adjoin]])
  (:require [protobuf.codec :as protobuf]))

(defn protobuf-codec [proto reduce-fn]
  (let [revisioned (-> (protobuf/protobuf-codec proto :repeated true)
                       (revisioned-codec reduce-fn))]
    (if (= adjoin reduce-fn)
      (let [full (protobuf/protobuf-codec proto)]
        (fn [{:keys [revision]}]
          (if (nil? revision)
            full
            (revisioned revision))))
      (fn [{:keys [revision]}]
        (revisioned revision)))))
