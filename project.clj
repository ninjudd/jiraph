(defproject jiraph "0.6.1-SNAPSHOT"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [clojure-useful "0.3.3"]
                 [masai "0.5.1-SNAPSHOT"]
                 [retro "0.5.0"]]
  :dev-dependencies [[clojure-protobuf "0.3.4"]]
  :tasks [protobuf.tasks]
  :java-compile {:target "1.7"})
