(ns flatland.jiraph.layer.masai
  (:use [flatland.jiraph.layer :as layer
         :only [EnumerateIds Optimized Historical Basic Layer ChangeLog Schema dispatch-update]]
        [flatland.jiraph.codex :only [encode decode]]
        [flatland.jiraph.layer.masai-common :only [implement-ordered revision-to-read revision-key?]]
        [flatland.retro.core :only [Transactional Revisioned OrderedRevisions
                                    at-revision txn-begin! txn-commit! txn-rollback!]]
        [flatland.useful.utils :only [if-ns adjoin returning map-entry verify]]
        [flatland.useful.map :only [update-in* assoc-in* into-map]]
        [flatland.useful.seq :only [find-with assert-length]]
        [flatland.useful.state :only [volatile put!]]
        [flatland.useful.fn :only [as-fn fix given]]
        [flatland.useful.datatypes :only [assoc-record]]
        [flatland.io.core :only [bufseq->bytes]]
        flatland.useful.debug)
  (:require [flatland.masai.db :as db]
            [flatland.jiraph.graph :as graph :refer [with-action]]
            [flatland.jiraph.formats.cereal :as cereal])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader
            DataOutputStream DataInputStream]
           [java.nio ByteBuffer]))

;;; A masai-layer "format" is a map containing, at least, a Gloss codec for encoding nodes,
;;; under the :codec key. Optional components:
;;; - a Schematic :schema
;;; - the :reduce-fn the codec uses when combining revisions
;;;   - this is used to optimize updates
;;; - a :revisions codec, for reading the list of revisions at which a node has been touched.

(defn write-format [layer node-id]
  ((:format-fn layer) {:id node-id :revision (:revision layer)}))

(defn read-format [layer node-id]
  ((:format-fn layer) {:id node-id :revision (revision-to-read layer)}))

(declare reset?)

(defn- revision-seq [format revision bytes]
  (when-let [rev-codec (:revisions format)]
    (let [revs (decode rev-codec bytes)]
      (distinct
       (if-not revision
         revs
         (take-while #(<= % revision) revs))))))

(defn- overwrite [layer id attrs]
  (let [{:keys [db append?]} layer
        format (write-format layer id)]
    (db/put! db (encode (:key-codec layer) id)
             (encode (:codec format) attrs))))

(defn- get-node* [layer id key not-found]
  (or (when-let [data (db/fetch (:db layer) key)]
        (not-empty (decode (:codec (read-format layer id))
                           data)))
      not-found))

(defrecord MasaiLayer [db revision max-written-revision append? format-fn key-codec]
  Object
  (toString [this]
    (pr-str this))

  EnumerateIds
  (node-id-seq [this opts]
    (layer/deny-sorted-seq opts)
    (map (partial decode key-codec)
         (remove revision-key? (db/key-seq db))))

  Basic
  (get-node [this id not-found]
    (get-node* this id
               (encode key-codec id)
               not-found))
  (update-in-node [this keyseq f args]
    (let [ioval (graph/simple-ioval this keyseq f args)]
      (ioval (dispatch-update keyseq f args
                              (fn assoc* [id value]
                                (fn [layer]
                                  (when append?
                                    (verify (not (db/exists? db (->> id
                                                                     (encode (:key-codec layer)))))
                                            "Can't overwrite in append mode"))
                                  (overwrite layer id value)))
                              (fn dissoc* [id]
                                (verify (not append?) "Can't dissoc nodes in append mode")
                                (fn [layer]
                                  (db/delete! db (encode key-codec id))))
                              (fn update* [id keys]
                                (if append?
                                  (do (verify (not (reset? this keyseq f))
                                              "Can't overwrite in append mode")
                                      (let [[attrs] (assert-length 1 args)]
                                        (fn [layer]
                                          (->> (assoc-in* {} keys attrs)
                                               (encode (:codec (write-format layer id)))
                                               (db/append! db (encode key-codec id))))))
                                  (fn [layer]
                                    (let [old (graph/get-node layer id)
                                          new (apply update-in* old keys f args)]
                                      (overwrite layer id new)))))))))

  Optimized
  (query-fn [this keyseq not-found f] nil)

  Layer
  (open [this]
    (db/open db))
  (close [this]
    (db/close db))
  (sync! [this]
    (db/sync! db))
  (optimize! [this]
    (db/optimize! db))
  (truncate! [this]
    (db/truncate! db)
    (put! max-written-revision nil))
  (same? [this other]
    (apply = (for [layer [this other]]
               (get-in layer [:db :opts :path]))))

  Schema
  (schema [this id]
    (:schema (read-format this id)))
  (verify-node [this id attrs]
    (try
      ;; do a fake write (does no I/O), to see if an exception would occur
      (encode (:codec (write-format this id)) attrs)
      (catch Exception _ false)))

  ChangeLog
  (get-revisions [this id]
    (when-let [data (db/fetch db (encode key-codec id))]
      (revision-seq (read-format (at-revision this nil) id) revision data)))

  Historical
  (node-history [this id]
    (when-let [data (db/fetch db (encode key-codec id))]
      (if-let [historical-codec (:historical (read-format this id))]
        (decode historical-codec data)
        (when-let [revisions (revision-seq (read-format (at-revision this nil) id)
                                           revision data)]
          (into (sorted-map)
                (for [revision revisions]
                  [revision (decode (:codec (read-format (at-revision this revision) id))
                                    data)]))))))

  ;; TODO this is stubbed, will need to work eventually
  (get-changed-ids [this rev]
    #{})

  Transactional
  (txn-begin! [this]
    (txn-begin! db))
  (txn-commit! [this]
    (txn-commit! db))
  (txn-rollback! [this]
    (put! max-written-revision nil)
    (txn-rollback! db))

  Revisioned
  (at-revision [this rev]
    (assoc-record this :revision rev))
  (current-revision [this]
    revision))

(implement-ordered MasaiLayer)

(if-ns (:require [flatland.masai.tokyo :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

(defn reset? [layer keyseq f]
  (not (when-first [id keyseq]
         (= f (:reduce-fn (write-format layer id))))))

(let [default-format-fn (cereal/revisioned-clojure-format adjoin)
      default-key-codec {:read #(String. ^bytes %)
                         :write #(.getBytes ^String %)}]
  ;; format-fn should be a function:
  ;; - accept as arg: a map containing {revision and node-id}
  ;; - return: a format (see doc for formats at the top of this file)
  (defn make [db & opts]
    (let [{:keys [write-mode format-fn key-codec]
           :or {write-mode :overwrite
                format-fn default-format-fn
                key-codec default-key-codec}}
          (into-map opts)]
      (MasaiLayer. (make-db db) nil (volatile nil)
                   (case write-mode
                     :append true
                     :overwrite false)
                   (as-fn format-fn)
                   key-codec))))

(defn temp-layer
  "Create a masai layer on a temporary file, deleting the file when the JVM exits.
   Returns a pair of [file layer]."
  [& args]
  (let [file (java.io.File/createTempFile "layer" "db")
        name (.getAbsolutePath file)]
    (returning [file (apply make name args)]
      (.deleteOnExit file))))

(def make-temp (comp second temp-layer))

(defmacro with-temp-layer [[binding & args] & body]
  `(let [[file# layer#] (temp-layer ~@args)
         ~binding layer#]
     (layer/open layer#)
     (returning ~@body
       (layer/close layer#)
       (.delete file#))))
