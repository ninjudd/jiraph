(ns flatland.jiraph.debug
  (:use flatland.jiraph.layer)
  (:require [flatland.retro.core :as retro :refer [Transactional txn-begin! txn-commit! txn-rollback!
                                          OrderedRevisions revision-range touch
                                          Revisioned current-revision at-revision]]
            [flatland.jiraph.graph :as graph]
            [flatland.useful.debug :refer [?]]
            [flatland.useful.map :refer [update]]
            [flatland.jiraph.wrapped-layer :refer [defwrapped update-wrap-read forward-reads]]
            [flatland.useful.datatypes :refer [assoc-record]]
            [clojure.pprint :refer [pprint]]
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

(defwrapped DebugLayer [layer layer-name hooks] []
  Basic
  (get-node     [this id not-found] (logged (get-node id not-found)))
  (update-in-node [this keyseq f args]
    (-> (log this 'update-in-node 'update-in-node (list* (vec keyseq)
                                                         (symbol (.getName (class f)))
                                                         args)
             #(update-in-node layer keyseq f args))
        (update-wrap-read forward-reads this layer)))

  Optimized
  (query-fn [this keyseq not-found f]
    (when-let [q (query-fn layer keyseq not-found f)]
      (fn [& args]
        (log this 'query-fn 'query-in (list* (vec keyseq)
                                             not-found
                                             (symbol (.getName (class f)))
                                             args)
             #(apply q args)))))

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

  OrderedRevisions
  (revision-range [this] (logged (revision-range)))
  (touch          [this] (logged (touch))))

(defn make* [base-layer layer-name log-functions]
  (DebugLayer. base-layer layer-name log-functions))

(defmacro make [base-layer layer-name & log-functions]
  `(make* ~base-layer '~layer-name '~(set log-functions)))

(defmacro ?rev
  ([ioval]
     `(?rev graph/get-in-node ~ioval))
  ([read ioval]
     `(let [io# ~ioval
            actions# (map #(-> (select-keys % [:layer :keyseq :f :args])
                               (update :layer get-in [:db :opts :path]))
                          (io# ~read))]
        (printf "%s is:%n" ~(pr-str ioval))
        (pprint actions#)
        io#)))