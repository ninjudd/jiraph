(ns jiraph.tokyo-database
  (:require jiraph.byte-database)
  (:import [tokyocabinet HDB]))

(def compress
  {:deflate HDB/TDEFLATE
   :bzip    HDB/TBZIP
   :tcbs    HDB/TTCBS})

(defn- tflags [opts]
  (bit-or
   (if (:large opts) HDB/TLARGE 0)
   (or (compress (:compress opts)) 0)))

(defn- oflags [opts]
  (reduce bit-or 0
    (list (if (:readonly opts) HDB/OREADER HDB/OWRITER)
          (if (:create   opts) HDB/OCREAT  0)
          (if (:truncate opts) HDB/OTRUNC  0)
          (if (:tsync    opts) HDB/OTSYNC  0)
          (if (:nolock   opts) HDB/ONOLCK  0)
          (if (:noblock  opts) HDB/OLCKNB  0))))

(defmacro str->bytes [s]
  `(bytes (.getBytes (str ~s))))

(defmacro check [form]
  `(or ~form
       (case (.ecode ~'hdb)
         ~HDB/EKEEP false
         ~HDB/ENOREC false
         (throw (java.io.IOException. (.errmsg ~'hdb) )))))

(defn- key-seq* [hdb]
  (lazy-seq
   (if-let [key (.iternext2 hdb)]
     (cons key (key-seq* hdb))
     nil)))

(deftype TokyoDatabase [#^HDB hdb opts]
  jiraph.byte-database/ByteDatabase

  (open [db]
    (let [path (:path opts)
          bnum (or (:bnum opts)  0)
          apow (or (:apow opts) -1)
          fpow (or (:fpow opts) -1)]
      (check (.tune hdb bnum apow fpow (tflags opts)))
      (check (.open hdb path (oflags opts)))))

  (close [db] (.close hdb))
  (sync! [db] (.sync  hdb))
  
  (get [db key] (.get  hdb (str->bytes key)))
  (len [db key] (.vsiz hdb (str->bytes key)))

  (key-seq [db]
    (.iterinit hdb)
    (key-seq* hdb))
    
  (add!    [db key val] (check (.putkeep hdb (str->bytes key) (bytes val))))
  (put!    [db key val] (check (.put     hdb (str->bytes key) (bytes val))))
  (append! [db key val] (check (.putcat  hdb (str->bytes key) (bytes val))))
  (inc!    [db key i]   (.addint hdb (str->bytes key) i))
  
  (delete!   [db key] (check (.out    hdb (str->bytes key))))
  (truncate! [db]     (check (.vanish hdb)))

  (txn-begin  [db] (.tranbegin  hdb))
  (txn-commit [db] (.trancommit hdb))
  (txn-abort  [db] (.tranabort  hdb)))

(defn make [opts]
  (TokyoDatabase. (HDB.) opts))