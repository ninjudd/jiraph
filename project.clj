(defproject org.flatland/jiraph "0.8.3-SNAPSHOT"
  :description "embedded graph db library for clojure"
  :url "https://github.com/flatland/jiraph"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/core.match "0.2.0-alpha10"]
                 [org.flatland/masai "0.8.0-SNAPSHOT"]
                 [org.flatland/useful "0.8.9-SNAPSHOT"]
                 [org.flatland/cereal "0.2.1-SNAPSHOT"]
                 [org.flatland/ordered "1.3.3-SNAPSHOT"]
                 [org.flatland/schematic "0.1.0-SNAPSHOT"]
                 [org.flatland/retro "0.7.2-SNAPSHOT"]
                 [org.flatland/io "0.2.2-SNAPSHOT"]
                 [org.flatland/ego "0.2.0-SNAPSHOT"]
                 [org.flatland/protobuf "0.6.3-SNAPSHOT"]
                 [slingshot "0.10.3"]]
  :plugins [[lein-protobuf "0.2.1"]]
  :profiles {:1.5 {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :dev {:dependencies [[org.flatland/tokyocabinet "1.24.6"]
                                  [unk "0.9.3"]]}}
  :aliases {"testall" ["with-profile" "dev,default:dev,1.5,default" "test"]}
  :repositories {"sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}})
