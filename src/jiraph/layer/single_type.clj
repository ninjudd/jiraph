(ns jiraph.layer.single-type
  (:use jiraph.wrapped-layer
        [useful.utils :only [map-entry]])
  (:require [jiraph.layer :as layer :refer [dispatch-update same?]]
            [jiraph.graph :as graph :refer [update-in-node]]
            [jiraph.masai-common :refer [bytes->long long->bytes]]))

(defn update-keyseq-id [keyseq encode]
  (cons (encode (first keyseq))
        (rest keyseq)))

(defwrapped SingleTypeLayer [layer encode decode]
  layer/Basic
  (get-node [this id not-found]
    (layer/get-node layer (encode id) not-found))
  (update-in-node [this keyseq f args]
    (-> (dispatch-update keyseq f args
                         (fn assoc* [id value]
                           (update-in-node layer [] assoc (encode id) value))
                         (fn dissoc* [id]
                           (update-in-node layer [] dissoc (encode id)))
                         (fn update* [id keys]
                           (apply update-in-node layer (cons (encode id) keys) f args)))
        (update-wrap-read (fn [read]
                            (fn [layer' keyseq & [not-found]]
                              (if (same? layer' this)
                                (read layer
                                      (update-keyseq-id keyseq encode)
                                      not-found)
                                (read layer' keyseq not-found)))))))

  layer/Optimized
  (query-fn [this keyseq not-found f]
    (layer/query-fn layer (update-keyseq-id keyseq encode) not-found f))

  layer/SortedEnumerate
  (node-id-subseq [this cmp start]
    (map decode (layer/node-id-subseq layer cmp (encode start))))
  (node-subseq [this cmp start]
    (for [[id attrs] (layer/node-subseq layer cmp (encode start))]
      (map-entry (decode id) attrs)))

  layer/Enumerate
  (node-id-seq [this]
    (map decode (layer/node-id-seq layer)))
  (node-seq [this]
    (for [[id attrs] (layer/node-seq layer)]
      (map-entry (decode id) attrs)))

  layer/ChangeLog
  (get-revisions [this id]
    (layer/get-revisions layer (encode id)))
  (get-changed-ids [this rev]
    (map decode (layer/get-changed-ids layer rev))))

(defn make [layer type]
  (let [type-prefix (str (name type) "-")
        type-len (count type-prefix)] ;; if type is :person, drop the "person-" suffix
    (SingleTypeLayer. layer
                      (fn encode [^String id]
                        (String. (long->bytes (Long/parseLong (subs id type-len)))))
                      (fn decode [^String key]
                        (str type-prefix (bytes->long (.getBytes key)))))))
