(defproject jiraph "0.1.3-SNAPSHOT" 
  :description "an embedded graph database for clojure"
  :dependencies [[tokyocabinet "1.23-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-SNAPSHOT"]
                 [org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [clojure-protobuf "0.1.0-SNAPSHOT"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]
                     [clojure-protobuf "0.1.0-SNAPSHOT"]])


(use 'clojure.contrib.with-ns)
(require 'leiningen.test)

(with-ns 'leiningen.test
  (defn proto [project]
    (try (require 'classlojure)
         (require 'leiningen.proto)
         ((ns-resolve 'leiningen.proto 'proto) project)
         (catch java.io.FileNotFoundException e
           (println "you must run 'lein deps' first")
           (System/exit 1))))
  
  (def *test* test)
  
  (defn test
    ([project & namespaces]
       (proto project)
       (if (nil? namespaces)
         (*test* project)
         (*test* project (apply str namespaces))))))
