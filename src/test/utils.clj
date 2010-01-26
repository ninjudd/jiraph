(ns test.utils
  (:use [clojure.contrib java-utils])
  (:import [java.io File IOException FileNotFoundException]))

; taken from cupboard.utils

(defn make-temp-dir []
  (let [tf (File/createTempFile "temp-" (str (java.util.UUID/randomUUID)))]
    (when-not (.delete tf)
      (throw (IOException.
              (str "Failed to delete temporary file " (.getAbsolutePath tf)))))
    (when-not (.mkdir tf)
      (throw (IOException.
              (str "Failed to create temporary directory " (.getAbsolutePath tf)))))
    (.getAbsolutePath tf)))

(defn rmdir-recursive [dir]
  (let [#^File dir (file dir)]
    (when-not (.exists dir)
      (throw (FileNotFoundException.
              (str "Not found for deletion: " (.getAbsolutePath dir)))))
    (when (.isDirectory dir)
      (doseq [#^String x (.list dir)] (rmdir-recursive (File. dir x))))
    (when-not (.delete dir)
      (throw (IOException. (str "Failed to delete " (.getAbsolutePath dir)))))))

(declare *db-path*)

(defn setup-db-path [f]
  (binding [*db-path* (make-temp-dir)]
    (f)
    (rmdir-recursive *db-path*)))
