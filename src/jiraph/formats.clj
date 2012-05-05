(ns jiraph.formats
  (:use [useful.experimental :only [lift-meta]]
        [useful.map :only [update update-each]]
        [useful.utils :only [copy-meta]])
  (:require [ego.core :as ego]
            [schematic.core :as schema]
            [jiraph.codex :as codex]))

(def reset-key :codec_reset)
(def revision-key :revisions)
(def len-key :proto_length)

(defn tidy-node [node]
  (-> node
      (dissoc reset-key len-key)
      (lift-meta revision-key)))

(defn revisions-codec [codec]
  ;; read revisions just by discarding the rest
  (codex/wrap codec
              nil ;; never write with this codec
              (comp revision-key meta)))

(defn resetting-codec [codec]
  (codex/wrap codec
              (fn [data] (assoc data reset-key true))
              identity))

(defn add-revisioning-modes [format]
  (let [{:keys [codec]} format]
    (-> format
        (assoc :revisions (revisions-codec codec)
               :reset     (resetting-codec codec))
        (update-each [:schema] schema/dissoc-fields revision-key))))

;; TODO accept codec+opts as well as codec-fn+reduce-fn
(defn revisioned-format [format-fn]
  ;; TODO take in a map of reduce-fn and (optionally) init-val
  (fn [{:keys [revision] :as opts}]
    (let [{:keys [reduce-fn] :as format} (format-fn opts)]
      (letfn [(reducer [acc x]
                (reduce-fn (if (reset-key x)
                             (select-keys acc [revision-key])
                             acc)
                           (dissoc x reset-key)))
              (combine [items]
                (when (seq items)
                  (tidy-node (or (reduce reducer items) {}))))
              (frame [pre-encode post-decode]
                (-> format
                    (update :codec codex/wrap
                            (comp list pre-encode)
                            (comp combine post-decode))
                    (add-revisioning-modes)))]
        (if revision
          (frame #(assoc % revision-key [revision])
                 (fn [vals]
                   ;; run BEFORE tidy-node, so revision is not in the meta but in the node
                   (take-while #(<= (first (revision-key %)) revision)
                               vals)))
          (frame identity identity))))))

(def ^{:dynamic true, :doc "When bound to false, codecs created by wrap-typing will ignore types."}
  *honor-layer-types* true)

(defn wrap-typing [format-fn accept-id?]
  (fn [opts]
    (when (or (not *honor-layer-types*)
              (accept-id? (:id opts)))
      (format-fn opts))))

(defn special-codec [format key]
  (or (get format key)
      (get format :codec)))
