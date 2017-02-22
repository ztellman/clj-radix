(ns clj-radix
  (:refer-clojure
    :exclude [remove merge merge-with update])
  (:require
    [primitive-math :as p]
    [clj-radix.utils :refer :all]
    [clj-radix.map :refer :all])
  (:import
    [java.lang.reflect
     Array]
    [clj_radix.map
     IRadixMap]))

;;;

(def ^:private ^:const NONE ::none)

(def ^:private ^:const obj-ary (class (object-array [])))

(definterface IRadixNode
  (merge [node f])
  (descend [^clojure.lang.Indexed ks ^long offset])
  (toNestedMap [])
  (entries [prefix])

  (assocIndexed [ks ^long offset ^long epoch v])
  (assocArray [ks ^long offset ^long epoch v])
  (assocString [ks ^long offset ^long epoch v])

  (dissocIndexed [ks ^long offset ^long epoch])
  (dissocArray [ks ^long offset ^long epoch])
  (dissocString [ks ^long offset ^long epoch])

  (updateIndexed [ks ^long offset ^long epoch f])
  (updateArray [ks ^long offset ^long epoch f])
  (updateString [ks ^long offset ^long epoch f])

  (getIndexed [ks ^long offset default])
  (getArray [ks ^long offset default])
  (getString [ks ^long offset default]))

(declare ^:private node)

(defmacro ^:private get*
  [[ks offset default] get-method matching-prefix ks-size ks-nth]
  `(let [idx# (~matching-prefix ~offset ~ks ~'prefix)]
      (if (p/== (p/- idx# ~offset) (Array/getLength ~'prefix))
        (if (p/== idx# ~ks-size)
          (if (identical? ~'value NONE)
            ~default
            ~'value)
          (if-let [^IRadixNode child# (lookup ~'children (~ks-nth ~ks idx#) nil)]
            (~get-method child# ~ks (p/inc idx#) ~default)
            ~default))
        ~default)))

(defmacro ^:private assoc*
  [[this ks offset epoch v]
   assoc-method
   drop-method
   matching-prefix
   ks-size
   ks-nth]
  `(let [ks-cnt# ~ks-size
         prefix-length# (Array/getLength ~'prefix)
         transient?# (p/== ~epoch ~'epoch)]
     (if (p/== ~offset ks-cnt#)

       ;; ks is empty, park ourselves here
       (if (p/== 0 prefix-length#)
         (if transient?#
           (do
             (set! ~'value ~v)
             ~this)
           (node nil ~v ~epoch ~'children))
         (node nil ~v ~epoch
           (radix-map
             (aget ~'prefix 0) (node (drop-array 1 ~'prefix) ~'value ~epoch ~'children))))

      (let [idx# (~matching-prefix ~offset ~ks ~'prefix)
            full-ks?# (p/== ks-cnt# idx#)
            full-prefix?# (p/== prefix-length# (p/- idx# ~offset))]
        (cond

          ;; no overlap, non-empty prefix
          (and (p/== ~offset idx#) (not full-prefix?#))
          (node nil NONE ~epoch
            (radix-map
              (~ks-nth ~ks idx#) (node (~drop-method (p/inc idx#) ~ks) ~v ~epoch nil)
              (aget ~'prefix 0) (node (drop-array 1 ~'prefix) ~'value ~epoch ~'children)))

          ;; ks is a substring
          full-ks?#
          (if full-prefix?#

            ;; perfect match, just overwrite
            (if transient?#
              (do
                (set! ~'value ~v)
                ~this)
              (node ~'prefix ~v ~epoch ~'children))

            ;; place the current node below the new one
            (node (~drop-method ~offset ~ks) ~v ~epoch
              (let [idx'# (p/- idx# ~offset)]
                (radix-map
                  (aget ~'prefix idx'#)
                  (node (drop-array (p/inc idx'#) ~'prefix) ~'value ~epoch ~'children)))))

          ;; prefix is a substring
          full-prefix?#
          (let [x# (~ks-nth ~ks idx#)
                ^IRadixNode child# (lookup ~'children x# nil)]
            (if (and transient?# (not (nil? ~'children)))
              (do
                (if (nil? child#)
                  (put! ~'children x# (node (~drop-method (p/inc idx#) ~ks) ~v ~epoch nil))
                  (let [child'# (~assoc-method child# ~ks (p/inc idx#) ~epoch ~v)]
                    (if (identical? child# child'#)
                      nil
                      (put! ~'children x# child'#))))
                ~this)
              (node ~'prefix ~'value ~epoch
                (put ~'children x#
                  (if (nil? child#)
                    (node (~drop-method (p/inc idx#) ~ks) ~v ~epoch nil)
                    (~assoc-method child# ~ks (p/inc idx#) ~epoch ~v))))))

          ;; partial overlap
          :else
          (node (take-array (p/- idx# ~offset) ~'prefix)
            NONE
            ~epoch
            (radix-map
              (~ks-nth ~ks idx#)
              (node (~drop-method (p/inc idx#) ~ks) ~v ~epoch nil)

              (aget ~'prefix (p/- idx# ~offset))
              (node (drop-array (p/inc (p/- idx# ~offset)) ~'prefix) ~'value ~epoch ~'children))))))))

(defmacro ^:private dissoc*
  [[this ks offset epoch default] dissoc-method matching-prefix ks-size ks-nth]
  `(let [idx# (~matching-prefix ~offset ~ks ~'prefix)
         cnt# ~ks-size
         transient?# (p/== ~epoch ~'epoch)]
     (if (p/== (p/- idx# ~offset) (Array/getLength ~'prefix))
       (if (p/== idx# cnt#)
         (if (nil? ~'children)
           nil
           (if transient?#
             (do
               (set! ~'value NONE)
               ~this)
             (node ~'prefix NONE ~epoch ~'children)))

         (let [x# (~ks-nth ~ks idx#)
               ^IRadixNode child# (lookup ~'children x# nil)]
           (if (nil? child#)
             ~this
             (let [child'# (~dissoc-method child# ~ks (p/inc idx#) ~epoch)]
               (if (identical? child# child'#)
                 (do
                   (put! ~'children x# child'#)
                   ~this)
                 (if (nil? child'#)
                   (if transient?#
                     (if (nil? (remove! ~'children x#))
                       nil
                       ~this)
                     (node ~'prefix ~'value ~epoch (remove ~'children x#)))
                   (node ~'prefix ~'value ~epoch
                     (put ~'children x# child'#))))))))
       ~this)))

(defmacro ^:private update*
  [[this ks offset epoch f] assoc-method update-method matching-prefix ks-size ks-nth]
  `(let [idx# (~matching-prefix ~offset ~ks ~'prefix)
         cnt# ~ks-size
         transient?# (p/== ~epoch ~'epoch)]
     (if (p/== (p/- idx# ~offset) (Array/getLength ~'prefix))
       (if (p/== idx# cnt#)
         (let [value'# (~f (if (identical? NONE ~'value) nil ~'value))]
           (if transient?#
             (do
               (set! ~'value value'#)
               ~this)
             (node ~'prefix value'# ~epoch ~'children)))

         (let [x# (~ks-nth ~ks idx#)
               ^IRadixNode child# (lookup ~'children x# nil)]
           (if (nil? child#)
             (~assoc-method ~this ~ks ~offset ~epoch (~f nil))
             (let [child'# (~update-method child# ~ks (p/inc idx#) ~epoch ~f)]
               (if (identical? child# child'#)
                 (do
                   (put! ~'children x# child'#)
                   ~this)
                 (node ~'prefix ~'value ~epoch (put ~'children x# child'#)))))))
       (~assoc-method ~this ~ks ~offset ~epoch (~f nil)))))

(deftype RadixNode
  [^objects prefix
   ^:volatile-mutable value
   ^long epoch
   ^IRadixMap children]

  IRadixNode

  (merge [this n f]
    (let [^RadixNode n n
          epoch' (p/inc epoch)
          idx (matching-prefix-array 0 prefix (.prefix n))
          cnt (Array/getLength prefix)
          cnt' (Array/getLength (.prefix n))]

      (cond (and (p/== idx cnt) (p/== idx cnt'))

        ;; match, do actual merge
        (let [value' (if (identical? value NONE)
                       (.value n)
                       (if (identical? (.value n) NONE)
                         value
                         (f value (.value n))))
              children' (reduce
                          (fn [children k]
                            (let [child' (lookup (.children n) k nil)]
                              (if-let [^RadixNode child (lookup children k nil)]
                                (put children k (.merge child child' f))
                                (put children k child'))))
                          children
                          (keys* (.children n)))]
          (node prefix value' epoch' children'))

        ;; place ourselves above the other node
        (p/== idx cnt)
        (node prefix value epoch'
          (let [n' (node (drop-array (p/inc idx) (.prefix n)) (.value n) epoch' (.children n))
                x (aget ^objects (.prefix n) idx)]
            (if-let [^RadixNode child (lookup children x nil)]
              (put children x (.merge child n' f))
              (put children x n'))))

        ;; place the other node above us
        (p/== idx cnt')
        (node (.prefix n) (.value n) epoch'
          (let [^RadixNode this' (node (drop-array (p/inc idx) prefix) value epoch' children)
                x (aget prefix idx)]
            (if-let [child (lookup (.children n) x nil)]
              (put (.children n) x (.merge this' child f))
              (put (.children n) x this'))))

        ;; no match, place node above both
        :else
        (node (take-array idx prefix) NONE epoch'
          (radix-map
            (aget prefix idx) (node (drop-array (p/inc idx) prefix) value epoch' children)
            (aget ^objects (.prefix n) idx) (node (drop-array (p/inc idx) (.prefix n)) (.value n) epoch' (.children n)))))))

  (toNestedMap [this]
    (let [ks (keys* children)]
      (reduce
        #(array-map %2 %1)
        (if ks
          (zipmap ks (map #(.toNestedMap ^IRadixNode (lookup children % nil)) ks))
          (if (identical? value NONE)
            {}
            value))
        (reverse prefix))))

  (descend [this ^clojure.lang.Indexed ks ^long offset]
    (let [idx (matching-prefix-indexed offset ks prefix)
          ks-cnt (.count ks)]
      (if (p/<= (p/- ks-cnt offset) (Array/getLength prefix))
        (node
          (drop-array (p/- ks-cnt offset) prefix)
          value
          epoch
          children)
        (when-let [^IRadixNode child (lookup children (.nth ks idx) nil)]
          (.descend child ks (p/inc idx))))))

  (entries [_ prefix']
    (let [prefix' (into prefix' prefix)
          ks (seq
               (mapcat
                 (fn [k]
                   (.entries
                     ^IRadixNode (lookup children k nil)
                     (conj prefix' k)))
                 (keys* children)))]
      (if (identical? NONE value)
        ks
        (conj ks (clojure.lang.MapEntry. prefix' value)))))

  (assocIndexed [this ks ^long offset ^long epoch' v]
    (assoc* [this ^clojure.lang.Indexed ks offset epoch' v]
      .assocIndexed
      drop-indexed
      matching-prefix-indexed
      (p/long (.count ^clojure.lang.Indexed ks))
      .nth))

  (assocArray [this ks ^long offset ^long epoch' v]
    (assoc* [this ^objects ks offset epoch' v]
      .assocArray
      drop-array
      matching-prefix-array
      (p/long (Array/getLength ks))
      aget))

  (assocString [this ks ^long offset ^long epoch' v]
    (assoc* [this ^CharSequence ks offset epoch' v]
      .assocString
      drop-string
      matching-prefix-string
      (p/long (.length ^CharSequence ks))
      char-at))

  (dissocIndexed [this ks ^long offset ^long epoch']
    (dissoc* [this ^clojure.lang.Indexed ks offset epoch']
      .dissocIndexed
      matching-prefix-indexed
      (p/long (.count ^clojure.lang.Indexed ks))
      .nth))

  (dissocArray [this ks ^long offset ^long epoch']
    (dissoc* [this ^objects ks offset epoch']
      .dissocArray
      matching-prefix-array
      (p/long (Array/getLength ks))
      aget))

  (dissocString [this ks ^long offset ^long epoch']
    (dissoc* [this ^CharSequence ks offset epoch']
      .dissocString
      matching-prefix-string
      (p/long (.length ^CharSequence ks))
      char-at))

  (updateIndexed [this ks ^long offset ^long epoch' f]
    (update* [this ^clojure.lang.Indexed ks offset epoch' f]
      .assocIndexed
      .updateIndexed
      matching-prefix-indexed
      (p/long (.count ^clojure.lang.Indexed ks))
      .nth))

  (updateArray [this ks ^long offset ^long epoch' f]
    (update* [this ^objects ks offset epoch' f]
      .assocArray
      .updateArray
      matching-prefix-array
      (p/long (Array/getLength ks))
      aget))

  (updateString [this ks ^long offset ^long epoch' f]
    (update* [this ^CharSequence ks offset epoch' f]
      .assocString
      .updateString
      matching-prefix-string
      (p/long (.length ^CharSequence ks))
      char-at))

  (getIndexed [this ks offset default]
    (get* [^clojure.lang.Indexed ks offset default]
      .getIndexed
      matching-prefix-indexed
      (p/long (.count ^clojure.lang.Indexed ks))
      .nth))

  (getArray [this ks offset default]
    (get* [^objects ks offset default]
      .getArray
      matching-prefix-array
      (p/long (Array/getLength ks))
      aget))

  (getString [this ks offset default]
    (get* [ks offset default]
      .getString
      matching-prefix-string
      (p/long (.length ^CharSequence ks))
      char-at)))

(let [empty-ary (object-array [])]
  (defn- node
    ([^long epoch]
       (RadixNode. empty-ary NONE epoch nil))
    ([prefix ^long epoch children]
       (RadixNode. (if (nil? prefix) empty-ary prefix) NONE epoch children))
    ([prefix value ^long epoch children]
       (RadixNode. (if (nil? prefix) empty-ary prefix) value epoch children))))


;;;

(defprotocol IRadixTree
  (update [m ks f])
  (update! [m ks f])
  (sub-tree [m ks])
  (->nested-map [m])
  (^:private merge- [m m' f]))

(defn merge-with
  [f & ms]
  (reduce
    (fn
      ([] nil)
      ([m] m)
      ([a b] (merge- a b f)))
    ms))

(defn merge
  [& ms]
  (apply merge-with (fn [a b] b) ms))

(declare ->transient)

(defmacro ^:private compile-if [test then else]
  (if (eval test)
    then
    else))

(deftype PersistentRadixTree
  [^RadixNode root
   ^long epoch
   meta]

  IRadixTree

  (->nested-map [this]
    (.toNestedMap root))

  (update [this ks f]
    (let [epoch' (p/inc epoch)]
      (PersistentRadixTree.
        (cond
          (instance? clojure.lang.Indexed ks) (.updateIndexed root ks 0 epoch' f)
          (instance? obj-ary ks) (.updateArray root ks 0 epoch' f)
          (instance? CharSequence ks) (.updateString root ks 0 epoch' f)
          :else (.updateIndexed root (vec ks) 0 epoch' f))
        epoch'
        meta)))

  (update! [this ks f]
    (throw (IllegalStateException. "update! can only be called on a transient radix tree")))

  (sub-tree [this ks]
    (PersistentRadixTree.
      (or (.descend root (vec ks) 0) (node (p/inc epoch)))
      (p/inc epoch)
      meta))

  (merge- [this m f]
    (PersistentRadixTree.
      (if (instance? PersistentRadixTree m)
        (.merge root (.root ^PersistentRadixTree m) f)
        (let [m' (into (empty this) m)]
          (.merge root (.root ^PersistentRadixTree m') f)))
      (p/inc epoch)
      meta))

  clojure.lang.IObj
  (meta [_] meta)
  (withMeta [_ m] (PersistentRadixTree. root epoch m))

  clojure.lang.MapEquivalence

  clojure.lang.IPersistentCollection

  (equiv [this x]
    (and (map? x) (= x (into {} this))))

  (cons [this o]
    (if (map? o)
      (reduce #(apply assoc %1 %2) this o)
      (if-let [[k v] (seq o)]
        (assoc this k v)
        this)))

  clojure.lang.Counted

  (count [this]
    (count (seq this)))

  clojure.lang.Seqable
  (seq [this]
    (.entries root []))

  clojure.core.protocols.CollReduce

  (coll-reduce
    [this f]
    (reduce f (seq this)))

  (coll-reduce
    [this f val#]
    (reduce f val# (seq this)))

  Object
  (hashCode [this]
    (reduce
      (fn [acc [k v]]
        (unchecked-add acc (bit-xor (.hashCode k) (.hashCode v))))
      0
      (seq this)))

  clojure.lang.IHashEq
  (hasheq [this]
    (compile-if (resolve 'clojure.core/hash-unordered-coll)
      (hash-unordered-coll this)
      (.hashCode this)))

  (equals [this x]
    (or (identical? this x)
      (and
        (map? x)
        (= x (into {} this)))))

  (toString [this]
    (str (into {} this)))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k default]
    (cond
      (instance? clojure.lang.Indexed k) (.getIndexed root k 0 default)
      (instance? obj-ary k) (.getArray root k 0 default)
      (instance? CharSequence k) (.getString root k 0 default)
      :else (.getIndexed root (vec k) 0 default)))

  clojure.lang.Associative
  (containsKey [this k]
    (not (identical? ::not-found (.valAt this k ::not-found))))

  (entryAt [this k]
    (let [v (.valAt this k ::not-found)]
      (when (not= v ::not-found)
        (clojure.lang.MapEntry. k v))))

  (assoc [this k v]
    (let [epoch' (p/inc epoch)]
      (PersistentRadixTree.
        (cond
          (instance? clojure.lang.Indexed k) (.assocIndexed root k 0 epoch' v)
          (instance? obj-ary k) (.assocArray root k 0 epoch' v)
          (instance? CharSequence k) (.assocString root k 0 epoch' v)
          :else (.assocIndexed root (vec k) 0 epoch' v))
        epoch'
        meta)))

  (empty [this]
    (PersistentRadixTree. (node 0) 0 nil))

  clojure.lang.IEditableCollection
  (asTransient [this]
    (->transient root (p/inc epoch) meta))

  java.util.Map
  (get [this k]
    (.valAt this k))
  (isEmpty [this]
    (empty? this))
  (size [this]
    (count this))
  (keySet [_]
    (->> (.entries root [])
      (map key)
      set))
  (put [_ _ _]
    (throw (UnsupportedOperationException.)))
  (putAll [_ _]
    (throw (UnsupportedOperationException.)))
  (clear [_]
    (throw (UnsupportedOperationException.)))
  (remove [_ _]
    (throw (UnsupportedOperationException.)))
  (values [this]
    (->> this seq (map second)))
  (entrySet [this]
    (->> this seq set))
  (iterator [this]
    (clojure.lang.SeqIterator. (seq this)))

  clojure.lang.IPersistentMap
  (assocEx [this k v]
    (if (contains? this k)
      (throw (Exception. "Key or value already present"))
      (assoc this k v)))
  (without [this k]
    (let [epoch' (p/inc epoch)]
      (PersistentRadixTree.
        (or
          (cond
            (instance? clojure.lang.Indexed k) (.dissocIndexed root k 0 epoch')
            (instance? obj-ary k) (.dissocArray root k 0 epoch')
            (instance? CharSequence k) (.dissocString root k 0 epoch')
            :else (.dissocIndexed root (vec k) 0 epoch'))
          (node epoch'))
        epoch'
        meta)))

  clojure.lang.IFn

  (invoke [this k]
    (.valAt this k))

  (invoke [this k default]
    (.valAt this k default)))

;;;

(deftype TransientRadixTree
  [^RadixNode root
   ^long epoch
   meta]

  IRadixTree

  (->nested-map [this]
    (.toNestedMap root))

  (update [this ks f]
    (throw (IllegalStateException. "update can only be called on a persistent radix tree")))

  (sub-tree [this ks]
    (throw (IllegalStateException. "sub-tree can only be called on a persistent radix tree")))

  (merge- [this m f]
    (throw (IllegalStateException. "merge can only be called on a persistent radix tree")))

  (update! [this ks f]
    (let [root' (cond
                  (instance? clojure.lang.Indexed ks) (.updateIndexed root ks 0 epoch f)
                  (instance? obj-ary ks) (.updateArray root ks 0 epoch f)
                  (instance? CharSequence ks) (.updateString root ks 0 epoch f)
                  :else (.updateIndexed root (vec ks) 0 epoch f))]
      (if (identical? root root')
        this
        (TransientRadixTree. root' epoch meta))))

  clojure.lang.IObj
  (meta [_] meta)
  (withMeta [_ m] (TransientRadixTree. root (p/inc epoch) meta))

  clojure.lang.MapEquivalence

  (equiv [this x]
    (and (map? x) (= x (into {} this))))

  clojure.lang.Counted

  (count [this]
    (count (seq this)))

  clojure.lang.Seqable
  (seq [this]
    (.entries root []))

  Object
  (hashCode [this]
    (reduce
      (fn [acc [k v]]
        (unchecked-add acc (bit-xor (hash k) (hash v))))
      0
      (seq this)))

  (equals [this x]
    (or (identical? this x)
      (and
        (map? x)
        (= x (into {} this)))))

  (toString [this]
    (str (into {} this)))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k default]
    (cond
      (instance? clojure.lang.Indexed k) (.getIndexed root k 0 default)
      (instance? obj-ary k) (.getArray root k 0 default)
      (instance? CharSequence k) (.getString root k 0 default)
      :else (.getIndexed root (vec k) 0 default)))

  clojure.lang.Associative
  (containsKey [this k]
    (not (identical? ::not-found (.valAt this k ::not-found))))

  (entryAt [this k]
    (let [v (.valAt this k ::not-found)]
      (when (not= v ::not-found)
        (clojure.lang.MapEntry. k v))))

  clojure.lang.ITransientMap

  (assoc [this k v]
    (let [root' (cond
                  (instance? clojure.lang.Indexed k) (.assocIndexed root k 0 epoch v)
                  (instance? obj-ary k) (.assocArray root k 0 epoch v)
                  (instance? CharSequence k) (.assocString root k 0 epoch v)
                  :else (.assocIndexed root (vec k) 0 epoch v))]
      (if (identical? root root')
        this
        (TransientRadixTree. root' epoch meta))))

  (conj [this o]
    (if (map? o)
      (reduce #(apply assoc! %1 %2) this o)
      (if-let [[k v] (seq o)]
        (assoc! this k v)
        this)))

  (persistent [_]
    (PersistentRadixTree. root (p/inc epoch) meta))

  (without [this k]
    (let [root' (cond
                  (instance? clojure.lang.Indexed k) (.dissocIndexed root k 0 epoch)
                  (instance? obj-ary k) (.dissocArray root k 0 epoch)
                  (instance? CharSequence k) (.dissocString root k 0 epoch)
                  :else (.dissocIndexed root (vec k) 0 epoch))]
      (if (identical? root root')
        this
        (TransientRadixTree. (or root' (node epoch)) epoch meta))))

  clojure.lang.IFn

  (invoke [this k]
    (.valAt this k))

  (invoke [this k default]
    (.valAt this k default)))

(defn- ->transient [root ^long epoch meta]
  (TransientRadixTree. root epoch meta))

(defn radix-tree
  ([]
     (PersistentRadixTree. (node 0) 0 nil))
  ([k v]
     (assoc (radix-tree) k v))
  ([k v & kvs]
     (loop [m (assoc (radix-tree) k v)
            s kvs]
       (if (empty? s)
         m
         (let [k (first s)
               s' (rest s)]
           (if (empty? s')
             (throw (IllegalArgumentException. "odd number of arguments to radix-tree"))
             (recur (assoc m k (first s')) (rest s'))))))))
