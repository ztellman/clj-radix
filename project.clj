(defproject clj-radix "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[primitive-math "0.1.3"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.6.0-beta1"]
                                  [criterium "0.4.3"]
                                  [collection-check "0.1.2"]
                                  [org.clojure/math.combinatorics "0.0.7"]]}}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default (complement :benchmark)
                   :benchmark :benchmark}
  :jvm-opts ^:replace ["-server"])
