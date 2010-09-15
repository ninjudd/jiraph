(ns jiraph.layer
  (:refer-clojure :exclude [count sync]))

(def *rev* nil)

(defprotocol Layer "Jiraph layer protocol"
  (open          [layer])
  (close         [layer])
  (sync          [layer])
  (count         [layer])
  (get-node      [layer id] [layer id rev])
  (get-meta      [layer id] [layer id rev])
  (node-exists?  [layer id] [layer id rev])
  (txn           [layer f])
  (add-node!     [layer id attrs])
  (update-node!  [layer id f args])
  (assoc-node!   [layer id attrs])
  (append-node!  [layer id attrs])
  (compact-node! [layer id])
  (delete-node!  [layer id])
  (truncate!     [layer]))

(defmacro transaction [layer & forms]
  `(txn ~layer (fn [] (do ~@forms))))

(defn all-revisions [layer id]
  (filter pos? (:rev (get-meta layer id))))

(defn revisions [layer id]
  (let [meta (get-meta layer id)]
    (reverse
     (take-while pos? (reverse (:rev meta))))))

(defn incoming [layer id]
  (:in (get-meta layer id)))
