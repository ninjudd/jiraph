(ns jiraph.reader-append-format
  (:use [useful :only [append]])
  (:require jiraph.byte-append-format))

(defn- read-seq []
  (lazy-seq
   (let [form (read *in* false ::EOF)]
     (when-not (= ::EOF form)
       (cons form (read-seq))))))

(defn- read-append [defaults str]
  (with-in-str str
    (apply merge-with append defaults (read-seq))))

(deftype ReaderAppendFormat [defaults]
  jiraph.byte-append-format/ByteAppendFormat
  
  (load [format data]
    (if data
      (read-append defaults (String. data))
      defaults))
  
  (load [format data len]
    (if data
      (read-append defaults (String. data 0 len))
      defaults))
  
  (dump [format node]
    (.getBytes (pr-str node))))

(defn make [& [defaults]]
  (ReaderAppendFormat. defaults))