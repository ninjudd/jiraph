(defproject jiraph "0.8.0-beta6"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.4.0"]
                 [useful "0.8.3-alpha3"]
                 [masai "0.7.0-alpha9"]
                 [cereal "0.2.0-alpha3"]
                 [ordered "1.2.2"]
                 [schematic "0.0.6"]
                 [retro "0.6.0-beta1"]
                 [io "0.2.0-beta2"]
                 [ego "0.1.7"]
                 [org.clojure/core.match "0.2.0-alpha9"]]
  :dev-dependencies [[protobuf "0.6.0-beta18"]
                     [tokyocabinet "1.24.3" :ext true]
                     [unk "0.9.3"]]
  :checksum-deps true ;; Tired of accidentally running with old deps
  :hooks [leiningen.protobuf])
