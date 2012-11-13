(ns flatland.jiraph.layer.encoded-key
  (:use flatland.jiraph.wrapped-layer
        [useful.map :only [update-each]]
        [useful.utils :only [map-entry]])
  (:require [flatland.jiraph.layer :as layer :refer [dispatch-update same?]]
            [flatland.jiraph.graph :as graph :refer [update-in-node]]
            [flatland.jiraph.masai-common :refer [bytes->long long->bytes]]))

(defn- update-keyseq-id [keyseq encode]
  (cons (encode (first keyseq))
        (rest keyseq)))

(defn- update-subseq-opts [opts encode]
  (update-each opts [:start-key :end-key]
               #(when % (encode %))))

(defwrapped EncodedKeyLayer [layer encode decode]
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
  (node-id-subseq [this opts]
    (map decode (layer/node-id-subseq layer (update-subseq-opts opts encode))))
  (node-subseq [this opts]
    (for [[id attrs] (layer/node-subseq layer (update-subseq-opts opts encode))]
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

(defn single-type-layer [layer type]
  (let [type-prefix (str (name type) "-")
        type-len (count type-prefix)] ;; if type is :person, drop the "person-" suffix
    (EncodedKeyLayer. layer
                      (fn encode [^String id]
                        (String. (long->bytes (Long/parseLong (subs id type-len)))))
                      (fn decode [^String key]
                        (str type-prefix (bytes->long (.getBytes key)))))))
