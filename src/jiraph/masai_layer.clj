(ns jiraph.masai-layer
  (:use [jiraph.layer :only [Enumerate Optimized Basic Layer ChangeLog Meta Preferences
                             node-id-seq meta-key meta-key?] :as layer]
        [retro.core   :only [WrappedTransactional Revisioned OrderedRevisions txn-wrap]]
        [clojure.stacktrace :only [print-cause-trace]]
        [useful.utils :only [if-ns adjoin returning]]
        [useful.seq :only [find-with]]
        [useful.fn :only [as-fn]]
        [useful.datatypes :only [assoc-record]]
        [gloss.io :only [encode decode]]
        [io.core :only [bufseq->bytes]])
  (:require [masai.db :as db]
<<<<<<< HEAD
            [cereal.format :as f]
            [cereal.reader :as reader-append-format])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader]
           [clojure.lang LineNumberingPushbackReader]))

(def meta-prefix     "_")
(def internal-prefix "__")
(def property-prefix "___")

(defn- meta-key [id]
  (str meta-prefix id))

(def count-key (str internal-prefix "count"))

(defn- property-key [key]
  (str property-prefix key))

(declare meta-len get-meta len meta-len append-meta! set-len! reset-len! inc-count! dec-count!)

(defrecord MasaiLayer [db format meta-format]
  jiraph.layer/Layer

  (open      [layer] (db/open      db))
  (close     [layer] (db/close     db))
  (sync!     [layer] (db/sync!     db))
  (optimize! [layer] (db/optimize! db))

  (node-count [layer]
    (db/inc! db count-key 0))

  (node-ids [layer]
    (remove #(.startsWith ^String % meta-prefix) (db/key-seq db)))

  (fields [layer]
    (f/fields format))

  (fields [layer subfields]
    (f/fields format subfields))

  (node-valid? [layer attrs]
    (try (f/encode format (make-node attrs))
         true
         (catch Exception e)))

  (get-property  [layer key]
    (if-let [bytes ^bytes (db/get db (property-key key))]
      (read-string (String. bytes))))

  (set-property! [layer key val]
    (let [bytes (.getBytes (pr-str val))]
      (db/put! db (property-key key) bytes)
      val))

  (get-node [layer id]
    (if *revision*
      (when-let [length (len layer id *revision*)]
        (f/decode format (db/get db id) 0 length))
      (f/decode format (db/get db id))))

  (node-exists? [layer id]
    (< 0 (len layer id *revision*)))

  (add-node! [layer id attrs]
    (let [node (make-node attrs)
          data ^bytes (f/encode format node)]
      (when (db/add! db id data)
        (inc-count! layer)
        (set-len! layer id (alength data))
        (f/decode format data))))

  (set-node! [layer id attrs]
    (let [data ^bytes (f/encode format (make-node attrs))]
      (db/put! db id data)
      (reset-len! layer id (alength data))
      (f/decode format data)))

  (delete-node! [layer id]
    (let [node (get-node layer id)]
      (when (db/delete! db id)
        (dec-count! layer)
        (reset-len! layer id)
        node)))

  (get-revisions [layer id] (:rev (get-meta layer (meta-key id) *revision*)))
  (get-incoming  [layer id] (:in  (get-meta layer (meta-key id) *revision*)))

  (add-incoming!  [layer id from-id] (append-meta! layer id {:in {from-id true}}  *revision*))
  (drop-incoming! [layer id from-id] (append-meta! layer id {:in {from-id false}} *revision*))

  (truncate! [layer] (db/truncate! db))

  jiraph.layer.Append

  (append-node! [layer id attrs]
    (when-not (empty? attrs)
      (let [len  (db/len db id)
            node (make-node attrs)
            data ^bytes (f/encode format node)]
        (db/append! db id data)
        (when (= -1 len)
          (inc-count! layer))
        (set-len! layer id (+ (max len 0) (alength data)))
        (f/decode format data))))

  retro.core/WrappedTransactional

  (txn-wrap [layer f]
    (retro.core/wrapped-txn f db))

  retro.core/Revisioned

  (get-revision [layer]
    (get-property layer :rev))

  (set-revision! [layer rev]
    (set-property! layer :rev rev)))
=======
            [jiraph.graph :as graph]
            [jiraph.codecs.cereal :as cereal])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream InputStreamReader
            DataOutputStream DataInputStream]
           [java.nio ByteBuffer]))

(defn- format-for [layer node-id]
  (get layer (cond (= node-id (meta-key layer "_layer")) :layer-meta-format
                   (meta-key? layer node-id) :node-meta-format
                   :else :node-format)))

;; drop leading _ - NB must undo the meta-key impl in MasaiLayer
(defn- main-node-id [meta-id]
  {:pre [(= "_" (first meta-id))]}
  (subs meta-id 1))

(defn- bytes->long [bytes]
  (-> bytes (ByteArrayInputStream.) (DataInputStream.) (.readLong)))

(defn- long->bytes [long]
  (-> (ByteArrayOutputStream. 8)
      (doto (-> (DataOutputStream.) (.writeLong long)))
      (.toByteArray)))

(let [revision-key "__revision"]
  (defn- save-maxrev [layer]
    (when-let [rev (:revision layer)]
      (db/put! (:db layer) revision-key (long->bytes rev))))
  (defn- read-maxrev [layer]
    (when-let [bytes (db/fetch (:db layer) revision-key)]
      (bytes->long bytes))))

(defrecord MasaiLayer [db revision append-only? node-format node-meta-format layer-meta-format]
  Meta
  (meta-key [this k]
    (str "_" k))
  (meta-key? [this k]
    (.startsWith ^String k "_"))

  Enumerate
  (node-id-seq [this]
    (remove #(meta-key? this %) (db/key-seq db)))
  (node-seq [this]
    (map #(graph/get-node this %) (node-id-seq this)))

  Basic
  (get-node [this id not-found]
    (if-let [data (db/fetch db id)]
      (decode ((format-for this id) {:revision revision})
              [(ByteBuffer/wrap data)])
      not-found))
  (assoc-node! [this id attrs]
    (letfn [(bytes [data]
              (bufseq->bytes (encode ((format-for this id) {:revision revision}) ;; TODO pass id
                                     data)))]
      (if append-only?
        (db/append! db id (bytes (assoc attrs :_reset true)))
        (db/put!    db id (bytes attrs)))))
  (dissoc-node! [this id]
    (db/delete! db id))

  Optimized
  (query-fn [this keyseq f] nil)
  (update-fn [this keyseq f]
    (when-let [[id & keys] (seq keyseq)]
      (let [encoder (format-for this id)]
        (when (= f (:reduce-fn (meta encoder)))
          (fn [m]
            (db/append! db id
                        (bufseq->bytes (encode (encoder {:revision revision})
                                               (if keys
                                                 (assoc-in {} keys m)
                                                 m))))
            {:old nil :new m})))))

  Layer
  (open [layer]
    (db/open db))
  (close [layer]
    (db/close db))
  (sync! [layer]
    (db/sync! db))
  (optimize! [layer]
    (db/optimize! db))
  (truncate! [layer]
    (db/truncate! db))

  ;; Schema
  ;; (fields [layer]
  ;;   (f/fields format))
  ;; (fields [layer subfields]
  ;;   (f/fields format subfields))
  ;; (node-valid? [layer attrs]
  ;;   (try (f/encode format (make-node attrs))
  ;;        true
  ;;        (catch Exception e)))

  ChangeLog
  (get-revisions [this id]
    (let [format (format-for this id)
          rev-codec-builder (-> format meta :revisions)
          rev-codec (rev-codec-builder {})]
      (when-let [data (db/fetch db id)]
        (let [revs (decode rev-codec [(ByteBuffer/wrap data)])]
          (if-not revision
            revs
            (take-while #(<= % revision) revs))))))

  ;; TODO this is stubbed, will need to work eventually
  (get-changed-ids [layer rev]
    #{})

  ;; TODO: Problems with implicit transaction-wrapping: we end up writing that the revision has
  ;; been applied, and refusing to do any more writing to that revision. What is the answer?
  WrappedTransactional
  (txn-wrap [this f]
    ;; todo *skip-writes* for past revisions
    (fn [^MasaiLayer layer]
      (let [db-wrapped (txn-wrap db ; let db wrap transaction, but call f with layer
                                 (fn [_]
                                   (returning (f layer)
                                     (save-maxrev layer))))]
        (db-wrapped (.db layer)))))

  Revisioned
  (at-revision [this rev]
    (assoc-record this :revision rev))
  (current-revision [this]
    revision)

  OrderedRevisions
  (max-revision [this]
    (read-maxrev this))

  Preferences
  (manage-changelog? [this] false) ;; TODO wish this were more granular
  (manage-incoming? [this] true)
  (single-edge? [this] ;; TODO accept option to (make)
    false))
>>>>>>> f/protocols

(if-ns (:require protobuf.core
                 [cereal.protobuf :as protobuf])
       (defn- make-protobuf [format]
         (when (instance? cereal.protobuf.ProtobufFormat format)
           (protobuf/make jiraph.Meta$Node)))
       (defn- make-protobuf [format]
         nil))

(if-ns (:require [masai.tokyo :as tokyo])
       (defn- make-db [db]
         (if (string? db)
           (tokyo/make {:path db :create true})
           db))
       (defn- make-db [db]
         db))

<<<<<<< HEAD
(defn make [db & [format meta-format]]
  (let [format (or format (reader-append-format/make))]
    (MasaiLayer.
     (make-db db) format
     (or meta-format
         (if (instance? cereal.reader.ReaderFormat format)
           (reader-append-format/make {:in #{} :rev [] :len [] :mrev [] :mlen []})
           (make-protobuf format))))))

(defn- get-meta [^MasaiLayer layer key rev]
  (let [db (.db layer)
        mf (.meta-format layer)]
    (if rev
      (let [len (meta-len layer key rev)]
        (when (< 0 len)
          (f/decode mf (db/get db key) 0 len)))
      (f/decode mf (db/get db key)))))

(defn- len
  "The byte length of the node at revision rev."
  [^MasaiLayer layer id rev]
  (if-not rev
    (db/len (.db layer) id)
    (let [meta (get-meta layer (meta-key id) nil)
          len  (find-with (partial >= rev)
                          (reverse (:rev meta))
                          (reverse (:len meta)))]
      (when (and len (<= 0 len))
        len))))

(defn- meta-len
  "The byte length of the meta node at revision rev."
  [^MasaiLayer layer key rev]
  (or (when rev
        (let [meta (get-meta layer key nil)]
          ;; Must shift meta lengths by one since they store the length of the previous revision.
          (find-with (partial >= rev)
                     (reverse (cons 0 (:mrev meta)))
                     (cons nil (reverse (:mlen meta))))))
      (db/len (.db layer) key)))

(defn- append-meta! [^MasaiLayer layer id attrs & [rev]]
  (let [db (.db layer)
        mf (.meta-format layer)
        key (meta-key id)]
    (->> (if rev
           (assoc attrs :mrev rev :mlen (db/len db key))
           attrs)
         (f/encode mf)
         (db/append! db key))))

(defn- set-len! [^MasaiLayer layer id len]
  (if *revision*
    (append-meta! layer id {:rev *revision* :len len})))

(defn- reset-len! [^MasaiLayer layer id & [len]]
  ;; Insert a length of -1 to indicate that all lengths before the current one are no longer valid.
  (append-meta! layer id
    (if (and *revision* len)
      {:rev [0 *revision*] :len [-1 len]}
      {:rev 0 :len -1})))

(defn- inc-count! [^MasaiLayer layer]
  (db/inc! (.db layer) count-key 1))

(defn- dec-count! [^MasaiLayer layer]
  (db/inc! (:db layer) count-key -1))
=======
(defn- codec-fn [codec default]
  (as-fn (or codec default)))

;; formats should be functions from revision (and optionally node-id) to codec.
;; plain old codecs will be accepted as well
(defn make [db & {{:keys [node meta layer-meta]} :formats,
                  :keys [assoc-mode] :or {assoc-mode :append}}]
  (let [[node-format meta-format layer-meta-format]
        (for [f [node meta layer-meta]]
          (codec-fn f (cereal/clojure-codec adjoin)))]
    (MasaiLayer. (make-db db) nil
                 (case assoc-mode
                   :append true
                   :overwrite false)
                 node-format, meta-format, layer-meta-format)))
>>>>>>> f/protocols
