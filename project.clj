(defproject clj-radix "0.1.0"
  :description "A radix tree"
  :url "https://github.com/ztellman/clj-radix"
  :license {:name "MIT License"
            :url "http://dd.mit-license.org/"}
  :dependencies [[primitive-math "0.1.4"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.6.0"]
                                  [criterium "0.4.3"]
                                  [collection-check "0.1.3"]
                                  [org.clojure/math.combinatorics "0.0.8"]
                                  #_[codox-md "0.2.0" :exclusions [org.clojure/clojure]]]}}
  :plugins [[codox "0.8.0"]]
  :codox {;:writer codox-md.writer/write-docs
          :include [clj-radix]}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default (complement :benchmark)
                   :benchmark :benchmark}
  :jvm-opts ^:replace ["-server"])
