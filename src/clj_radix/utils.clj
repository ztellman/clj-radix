(ns clj-radix.utils
  (:require
    [primitive-math :as p])
  (:import
    [java.lang.reflect Array]))

(defn char-at
  {:inline (fn [cs idx]
             `(Character. (.charAt ~(with-meta cs {:tag "CharSequence"}) (p/int ~idx))))}
  [^CharSequence cs ^long idx]
  (Character. (.charAt cs idx)))

(defmacro ^:private drop*
  [[n s] cnt-s nth-s]
  (let [ary-sym (gensym "ary")]
    `(let [cnt# ~cnt-s]
       (if (p/>= ~n cnt#)
         (Array/newInstance Object 0)
         (let [~ary-sym (Array/newInstance Object (p/int (p/- cnt# ~n)))]
           (dotimes [i# (p/- cnt# ~n)]
             (aset ~(with-meta ary-sym {:tag 'objects}) i# (~nth-s ~s (p/+ i# ~n))))
           ~ary-sym)))))

(defn drop-array
  [^long n ^objects s]
  (if (p/== 0 n)
    s
    (drop* [n s]
      (p/long (Array/getLength s))
      aget)))

(defn drop-indexed
  [^long n ^clojure.lang.Indexed s]
  (drop* [n s]
    (p/long (.count ^clojure.lang.Indexed s))
    .nth))

(defn drop-string
  [^long n ^CharSequence s]
  (drop* [n s]
    (p/long (.length ^CharSequence s))
    char-at))

(defn take-array
  [^long n ^objects s]
  (let [cnt (p/long (Array/getLength s))]
    (if (p/== 0 n)
      (Array/newInstance Object 0)
      (let [cnt' (Math/min cnt n)
            ^objects ary (Array/newInstance Object cnt')]
        (dotimes [i cnt']
          (aset ary i (aget s i)))
        ary))))

;;;

(defmacro ^:private matching-prefix* [basic-equals? [offset ks prefix] ks-size ks-lookup]
  (let [idx-sym (gensym "idx")]
    `(if (or (nil? ~ks) (nil? ~prefix))
       ~offset
       (let [cnt-ks# (p/- ~ks-size ~offset)
             cnt-prefix# (p/long (Array/getLength ~prefix))
             cnt# (Math/min cnt-ks# cnt-prefix#)]
         (loop [~idx-sym 0]
           (if (p/== cnt# ~idx-sym)
             (p/+ ~offset ~idx-sym)
             ~(if basic-equals?
                `(let [v# (~ks-lookup ~ks (p/+ ~offset ~idx-sym))
                       v'# (aget ~prefix ~idx-sym)]
                   (if (if (nil? v#)
                         (nil? v'#)
                         (.equals v# v'#))
                     (recur (p/inc ~idx-sym))
                     (p/+ ~offset ~idx-sym)))
                `(if (= (~ks-lookup ~ks (p/+ ~offset ~idx-sym))
                       (aget ~prefix ~idx-sym))
                   (recur (p/inc ~idx-sym))
                   (p/+ ~offset ~idx-sym)))))))))

(defn ^long matching-prefix-array
  ^long [^long offset ^objects ks ^objects prefix]
  (matching-prefix* false [offset ks prefix]
    (Array/getLength ks)
    aget))

(defn ^long matching-prefix-indexed
  ^long [^long offset ^clojure.lang.Indexed ks ^objects prefix]
  (matching-prefix* false [offset ks prefix]
    (p/long (.count ^clojure.lang.Indexed ks))
    .nth))

(defn ^long matching-prefix-string
  ^long [^long offset ^CharSequence ks ^objects prefix]
  (matching-prefix* false [offset ks prefix]
    (p/long (.length ^CharSequence ks))
    char-at))

(defn ^long matching-prefix-array*
  ^long [^long offset ^objects ks ^objects prefix]
  (matching-prefix* true [offset ks prefix]
    (Array/getLength ks)
    aget))

(defn ^long matching-prefix-indexed*
  ^long [^long offset ^clojure.lang.Indexed ks ^objects prefix]
  (matching-prefix* true [offset ks prefix]
    (p/long (.count ^clojure.lang.Indexed ks))
    .nth))

(defn ^long matching-prefix-string*
  ^long [^long offset ^CharSequence ks ^objects prefix]
  (matching-prefix* true [offset ks prefix]
    (p/long (.length ^CharSequence ks))
    char-at))
