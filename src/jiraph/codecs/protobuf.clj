(ns jiraph.codecs.protobuf
  (:use [jiraph.codecs :only [revisioned-codec]]
        [useful.utils :only [adjoin]])
  (:require [protobuf.codec :as protobuf]))

;; NB doesn't currently work if you do a full/optimized read with _reset keys.
;; plan is to fall back to non-optimized reads in that case, but support an
;; option to protobuf-codec to never try an optimized read if you expect _resets
(defn protobuf-codec
  ([proto]
     (protobuf-codec proto adjoin))
  ([proto reduce-fn]
     (let [revisioned (-> (protobuf/protobuf-codec proto :repeated true)
                          (revisioned-codec reduce-fn))]
       (if (= adjoin reduce-fn)
         (let [full (protobuf/protobuf-codec proto)]
           (fn [{:keys [revision]}]
             (if (nil? revision)
               full
               (revisioned revision))))
         (fn [{:keys [revision]}]
           (revisioned revision))))))
