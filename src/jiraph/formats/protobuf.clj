(ns jiraph.formats.protobuf
  (:use [jiraph.formats :only [revisioned-format tidy-node]]
        [useful.utils :only [adjoin]]
        [useful.map :only [keyed]])
  (:require [gloss.core :as gloss]
            [protobuf.codec :as protobuf]))

;; NB doesn't currently work if you do a full/optimized read with _reset keys.
;; plan is to fall back to non-optimized reads in that case, but support an
;; option to protobuf-codec to never try an optimized read if you expect _resets
(defn protobuf-format
  ([proto]
     (protobuf-format proto adjoin))
  ([proto reduce-fn]
     (let [schema       (protobuf/codec-schema proto)
           codec        (protobuf/protobuf-codec proto, :repeated true)
           proto-format (keyed [codec schema reduce-fn])
           revisioned   (revisioned-format (constantly proto-format))]
       (if (= adjoin reduce-fn)
         (let [base-codec (protobuf/protobuf-codec proto)
               codec (gloss/compile-frame base-codec identity tidy-node)
               full-format (keyed [codec schema reduce-fn])]
           (fn [{:keys [revision] :as opts}]
             (if (nil? revision)
               full-format
               (revisioned opts))))
         revisioned))))
