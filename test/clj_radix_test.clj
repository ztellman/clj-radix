(ns clj-radix-test
  (:use
    [clojure.test])
  (:require
    [clj-radix :as r]
    [collection-check :as check]
    [criterium.core :as c]
    [simple-check.generators :as gen]
    [simple-check.properties :as prop]
    [simple-check.clojure-test :as ct :refer (defspec)]
    [clojure.math.combinatorics :as comb]))

;;;

(def element-generator
  (gen/one-of
    [(gen/vector gen/int 0 10)
     (gen/such-that #(< (count %) 10) gen/string-ascii)]))

(def radix-tree-generator
  (gen/fmap
    (fn [ks]
      (into (r/radix-tree) ks))
    (gen/list (gen/tuple element-generator gen/int))))

(defspec equivalent-update 1e3
  (let [f #(if % (inc %) 1)]
    (prop/for-all [m radix-tree-generator k element-generator]
      (= (r/update m k f)
        (assoc m k (f (get m k)))))))

(defspec equivalent-merge 1e3
  (prop/for-all [a radix-tree-generator, b radix-tree-generator]
    (= (merge-with + a b) (r/merge-with + a b))))

;;;

(deftest test-map-like
  (check/assert-map-like 1e3 (r/radix-tree) (gen/vector gen/int 0 10) gen/int))

;;;

(defn dissoc-in
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(deftest ^:benchmark benchmark-map
  (let [ks (apply comb/cartesian-product (repeat 5 (range 10)))
        v (vec (range 5))]
    (let [m (reduce #(assoc-in %1 %2 nil) {} ks)]
      (println "-- dense deep normal map")
      (println "-- get")
      (c/quick-bench (get-in m v))
      (println "-- assoc")
      (c/quick-bench (assoc-in m v nil))
      (println "-- dissoc")
      (c/quick-bench (dissoc-in m v)))
    (let [m (reduce #(assoc %1 %2 nil) {} ks)]
      (println)
      (println "-- dense flattened normal map")
      (println "-- get")
      (c/quick-bench (get m v))
      (println "-- assoc")
      (c/quick-bench (assoc m v nil))
      (println "-- dissoc")
      (c/quick-bench (dissoc m v)))
    (let [m (reduce #(assoc %1 %2 nil) (r/radix-tree) ks)]
      (println)
      (println "-- dense deep radix map")
      (println "-- get")
      (c/quick-bench (get m v))
      (println "-- assoc")
      (c/quick-bench (assoc m v nil))
      (println "-- assoc!")
      (let [m (assoc! (transient m) v nil)]
        (c/quick-bench (assoc! m v nil)))
      (println "-- dissoc")
      (c/quick-bench (dissoc m v))
      (println "-- dissoc!")
      (let [m (assoc! (transient m) v nil)]
        (c/quick-bench (dissoc! m v)))))

  (let [k (vec (range 10))]
    (let [m (assoc-in {} k nil)]
      (println "-- sparse deep normal map")
      (println "-- get")
      (c/quick-bench (get-in m k))
      (println "-- assoc")
      (c/quick-bench (assoc-in m k nil))
      (println "-- dissoc")
      (c/quick-bench (dissoc-in m k)))
    (let [m (assoc {} k nil)]
      (println)
      (println "-- sparse flattened normal map")
      (println "-- get")
      (c/quick-bench (get m k))
      (println "-- assoc")
      (c/quick-bench (assoc m k nil))
      (println "-- dissoc")
      (c/quick-bench (dissoc m k)))
    (let [m (assoc (r/radix-tree) k nil)]
      (println)
      (println "-- sparse deep radix map")
      (println "-- get")
      (c/quick-bench (get m k))
      (println "-- assoc")
      (c/quick-bench (assoc m k nil))
      (println "-- assoc!")
      (let [m (assoc! (transient m) k nil)]
        (c/quick-bench (assoc! m k nil)))
      (println "-- dissoc")
      (c/quick-bench (dissoc m k))
      (println "-- dissoc!")
      (let [m (assoc! (transient m) k nil)]
        (c/quick-bench (dissoc! m k))))))
