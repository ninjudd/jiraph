(defproject org.flatland/jiraph "0.12.3-SNAPSHOT"
  :description "embedded graph db library for clojure"
  :url "https://github.com/flatland/jiraph"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/core.match "0.2.0-alpha11"]
                 [org.flatland/masai "0.8.0"]
                 [org.flatland/useful "0.10.4"]
                 [org.flatland/cereal "0.3.0"]
                 [org.flatland/ordered "1.4.0"]
                 [org.flatland/schematic "0.1.0"]
                 [org.flatland/retro "0.8.0"]
                 [org.flatland/io "0.3.0"]
                 [org.flatland/ego "0.2.0"]
                 [slingshot "0.10.3"]]
  :plugins [[lein-protobuf "0.3.1"]]
  :profiles {:1.5 {:dependencies [[org.clojure/clojure "1.5.0"]]}
             :dev {:dependencies [[org.flatland/protobuf "0.7.2"]
                                  [org.flatland/tokyocabinet "1.24.6"]
                                  [org.clojure/core.memoize "0.5.2"]]}}
  :aliases {"testall" ["with-profile" "dev,default:dev,1.5,default" "test"]}
  :repositories {"sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}})
