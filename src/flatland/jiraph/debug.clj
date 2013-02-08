(ns flatland.jiraph.debug
  (:use flatland.jiraph.layer)
  (:require [flatland.retro.core :as retro :refer [Transactional txn-begin! txn-commit! txn-rollback!
                                          OrderedRevisions max-revision touch
                                          Revisioned current-revision at-revision]]
            [flatland.jiraph.wrapped-layer :refer [Wrapped]]
            [flatland.useful.datatypes :refer [assoc-record]]
            [clojure.string :as s]))

(defn- log [layer f-name display-name args thunk]
  (if (contains? (:hooks layer) f-name)
    (let [call-name (gensym 'call)
          label (str (:layer-name layer)
                     (when-let [rev (retro/current-revision layer)]
                       (format "(rev %s)" rev)))
          arg-str (s/join " " (cons label (map pr-str args)))]
      (printf "%s>(%s %s)\n" call-name display-name arg-str)
      (doto (thunk)
        (->> (printf "%s<%s\n" call-name))))
    (thunk)))

(defmacro logged [[f & args]]
  `(log ~'this '~f '~f [~@args]
        #(~f ~'layer ~@args)))

(defrecord DebugLayer [layer layer-name hooks]
  Object
  (toString [this] (pr-str this))

  Wrapped
  (unwrap [this] layer)

  Enumerate
  (node-id-seq [this] (logged (node-id-seq)))
  (node-seq    [this] (logged (node-seq)))

  SortedEnumerate
  (node-id-subseq [this cmp start] (logged (node-id-subseq cmp start)))
  (node-subseq    [this cmp start] (logged (node-subseq cmp start)))

  Basic
  (get-node     [this id not-found] (logged (get-node id not-found)))
  (assoc-node!  [this id attrs]     (logged (assoc-node! id attrs)))
  (dissoc-node! [this id]           (logged (dissoc-node! id)))

  Optimized
  (query-fn [this keyseq not-found f]
    (when-let [q (query-fn layer keyseq not-found f)]
      (fn [& args]
        (log this 'query-fn 'query-in (list* (vec keyseq)
                                             not-found
                                             (symbol (.getName (class f)))
                                             args)
             #(apply q args)))))
  (update-fn [this keyseq f]
    (when-let [u (update-fn layer keyseq f)]
      (fn [& args]
        (log this 'update-fn 'update-in (list* (vec keyseq)
                                               (symbol (.getName (class f)))
                                               args)
             #(apply u args)))))

  Layer
  (open       [this] (logged (open)))
  (close      [this] (logged (close)))
  (sync!      [this] (logged (sync!)))
  (optimize!  [this] (logged (optimize!)))
  (truncate!  [this] (logged (truncate!)))

  Schema
  (schema      [this id]       (logged (schema id)))
  (verify-node [this id attrs] (logged (verify-node id attrs)))

  ChangeLog
  (get-revisions   [this id]  (logged (get-revisions id)))
  (get-changed-ids [this rev] (logged (get-changed-ids rev)))

  Transactional
  (txn-begin!    [this] (logged (txn-begin!)))
  (txn-commit!   [this] (logged (txn-commit!)))
  (txn-rollback! [this] (logged (txn-rollback!)))

  Revisioned
  (at-revision      [this rev] (assoc-record this :layer (at-revision layer rev)))
  (current-revision [this]     (current-revision layer))

  OrderedRevisions
  (max-revision [this] (logged (max-revision)))
  (touch        [this] (logged (touch)))

  Preferences
  (manage-changelog? [this] (logged (manage-changelog?)))
  (manage-incoming?  [this] (logged (manage-incoming?)))
  (single-edge?      [this] (logged (single-edge?))))

(defn make* [base-layer layer-name log-functions]
  (DebugLayer. base-layer layer-name log-functions))

(defmacro make [base-layer layer-name & log-functions]
  `(make* ~base-layer '~layer-name '~(set log-functions)))
