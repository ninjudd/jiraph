(ns jiraph.codecs
  (:use [gloss.core.protocols :only [Reader Writer read-bytes write-bytes sizeof]]
        [useful.experimental :only [lift-meta]]
        [useful.map :only [update]]
        [useful.utils :only [copy-meta]]
        [gloss.core :only [compile-frame]])
  (:require [ego.core :as ego]
            [schematic.core :as schema]))

(def reset-key :codec_reset)
(def revision-key :revisions)
(def len-key :proto_length)

(defn tidy-node [node]
  (-> node
      (dissoc reset-key len-key)
      (lift-meta revision-key)))

(defn revisions-codec [codec]
  ;; read revisions just by discarding the rest
  (compile-frame codec
                 nil ;; never write with this codec
                 (comp revision-key meta)))

(defn resetting-codec [codec]
  (compile-frame codec
                 (fn [data] (assoc data reset-key true))
                 identity))

(defn codec-meta [codec base-codec reduce-fn]
  (with-meta codec
    {:reduce-fn reduce-fn
     :revisions (revisions-codec codec)
     :reset     (resetting-codec codec)
     :schema    (-> (:schema (meta base-codec))
                    (schema/dissoc-fields revision-key))}))

;; TODO accept codec+opts as well as codec-fn+reduce-fn
(defn revisioned-codec [codec-fn reduce-fn] ;; Codec -> (a -> a) -> (opts -> Codec) ;; TODO fix
  ;; TODO take in a map of reduce-fn and (optionally) init-val
  (letfn [(reducer [acc x]
            (reduce-fn (if (reset-key x)
                         (select-keys acc [revision-key])
                         acc)
                       (dissoc x reset-key)))
          (combine [items]
            (when (seq items)
              (tidy-node (or (reduce reducer items) {}))))]
    (fn [{:keys [revision] :as opts}]
      (let [frame (fn [pre-encode post-decode]
                    (let [codec (codec-fn opts)]
                      (-> codec
                          (compile-frame (comp list pre-encode)
                                         (comp combine post-decode))
                          (codec-meta codec reduce-fn))))]
        (if revision
          (frame #(assoc % revision-key [revision])
                 (fn [vals]
                   ;; run BEFORE tidy-node, so revision is not in the meta but in the node
                   (take-while #(<= (first (revision-key %)) revision)
                               vals)))
          (frame identity identity))))))

(def ^{:dynamic true, :doc "When bound to false, codecs created by wrap-typing will ignore types."}
  *honor-layer-types* true)

(defn wrap-typing [codec-fn accept-id?]
  (-> (fn [{:keys [id] :as opts}]
        (when (or (not *honor-layer-types*)
                  (accept-id? id))
          (codec-fn opts)))
      (copy-meta codec-fn)))

(defn special-codec [codec key]
  (get (meta codec) key codec))
