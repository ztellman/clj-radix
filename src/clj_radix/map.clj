(ns clj-radix.map
  (:refer-clojure :exclude [remove])
  (:import
    [java.util HashMap]))

(definterface IRadixMap
  (keys [])
  (put [k v])
  (putBang [k v])
  (remove [k])
  (removeBang [k])
  (lookup [k default]))

(deftype RadixMap [^HashMap m]
  IRadixMap
  (keys [_] (.keySet m))
  (put [_ k v] (RadixMap. (doto (HashMap. m) (.put k v))))
  (putBang [this k v] (.put m k v) this)
  (remove [_ k] (RadixMap. (doto (HashMap. m) (.remove k))))
  (removeBang [this k] (.remove m k) this)
  (lookup [_ k default] (if (.containsKey m k) (.get m k) default)))

(defn radix-map
  ([]
     (RadixMap. (HashMap.)))
  ([a b]
     (RadixMap. (doto (HashMap.) (.put a b))))
  ([a b c d]
     (RadixMap. (doto (HashMap.) (.put a b) (.put c d)))))

(defmethod print-method RadixMap [^RadixMap m w]
  (print-method (into {} (.m m)) w))

(defn put [^IRadixMap m k v]
  (if (nil? m)
    (radix-map k v)
    (.put m k v)))

(defn put! [^IRadixMap m k v]
  (if (nil? m)
    (radix-map k v)
    (.putBang m k v)))

(defn remove [^IRadixMap m k]
  (if (nil? m)
    nil
    (.remove m k)))

(defn remove! [^IRadixMap m k]
  (if (nil? m)
    nil
    (.removeBang m k)))

(defn lookup [^IRadixMap m k default]
  (if (nil? m)
    default
    (.lookup m k default)))

(defn keys* [^IRadixMap m]
  (if (nil? m)
    nil
    (.keys m)))
