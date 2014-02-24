(ns clj-radix
  (:refer-clojure
    :exclude [remove])
  (:require
    [primitive-math :as p])
  (:import
    [java.lang.reflect
     Array]
    [java.util
     HashMap]))

;;;

(def ^:private ^:const obj-ary (class (object-array [])))

(defn- ^long len ^long [^objects ary]
  (if (nil? ary)
    0
    (Array/getLength ary)))

(defn ^Character char-at [^CharSequence cs ^long idx]
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

(defn- drop-array
  [^long n ^objects s]
  (if (p/== 0 n)
    s
    (drop* [n s]
      (p/long (Array/getLength s))
      aget)))

(defn- drop-indexed
  [^long n ^clojure.lang.Indexed s]
  (drop* [n s]
    (p/long (.count ^clojure.lang.Indexed s))
    .nth))

(defn- take-array
  [^long n ^objects s]
  (let [cnt (p/long (Array/getLength s))]
    (if (p/== 0 n)
      (Array/newInstance Object 0)
      (let [cnt' (Math/min cnt n)
            ^objects ary (Array/newInstance Object cnt')]
        (dotimes [i cnt']
          (aset ary i (aget s i)))
        ary))))

(defn- drop*
  [^long n s]
  (if (p/>= n (count s))
    nil
    (if (instance? obj-ary s)
      (let [cnt (len s)
            ^objects ary (object-array (p/- cnt n))]
        (dotimes [i (p/- cnt n)]
          (aset ary i (aget ^objects s (p/+ i n))))
        ary)
      (let [cnt (count s)
            ^objects ary (object-array (p/- cnt n))]
        (dotimes [i (p/- cnt n)]
          (aset ary i (nth s (p/+ i n))))
        ary))))

(defn- take*
  [^long n s]
  (if (p/== 0 n)
    nil
    (let [^objects ary (object-array n)]
      (if (instance? obj-ary s)
        (dotimes [i n]
          (aset ary i (aget ^objects s i)))
        (dotimes [i n]
          (aset ary i (nth s i))))
      ary)))

;;;

(defmacro ^:private matching-prefix* [[offset ks prefix] ks-size ks-lookup]
  `(if (or (nil? ~ks) (nil? ~prefix))
     ~offset
     (let [cnt-ks# (p/- ~ks-size ~offset)
           cnt-prefix# (p/long (Array/getLength ~prefix))
           cnt# (Math/min cnt-ks# cnt-prefix#)]
       (loop [idx# 0]
         (if (p/== cnt# idx#)
           (p/+ ~offset idx#)
           (if (= (~ks-lookup ~ks (p/+ ~offset idx#)) (aget ~prefix idx#))
             (recur (p/inc idx#))
             (p/+ ~offset idx#)))))))

(defn- ^long matching-prefix
  ^long [^long offset ks ^objects prefix]
  (matching-prefix* [offset ks prefix]
    (p/long (count ks))
    nth))

(defn- ^long matching-prefix-array
  ^long [^long offset ^objects ks ^objects prefix]
  (matching-prefix* [offset ks prefix]
    (Array/getLength ks)
    aget))

(defn- ^long matching-prefix-indexed
  ^long [^long offset ^clojure.lang.Indexed ks ^objects prefix]
  (matching-prefix* [offset ks prefix]
    (p/long (.count ^clojure.lang.Indexed ks))
    .nth))

(defn- ^long matching-prefix-string
  ^long [^long offset ^CharSequence ks ^objects prefix]
  (matching-prefix* [offset ks prefix]
    (p/long (.length ^CharSequence ks))
    char-at))



;;;

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

(defn- put [^IRadixMap m k v]
  (if (nil? m)
    (radix-map k v)
    (.put m k v)))

(defn- put! [^IRadixMap m k v]
  (if (nil? m)
    (radix-map k v)
    (.putBang m k v)))

(defn- remove [^IRadixMap m k]
  (if (nil? m)
    nil
    (.remove m k)))

(defn- remove! [^IRadixMap m k]
  (if (nil? m)
    nil
    (.removeBang m k)))

(defn- lookup [^IRadixMap m k default]
  (if (nil? m)
    default
    (.lookup m k default)))

;;;

(def ^:private ^:const none ::none)

(definterface IRadixNode
  (epoch ^long [])
  (entries [prefix])
  (assoc [ks ^long offset ^long epoch v])
  (dissoc [ks ^long offset ^long epoch])
  (update [ks ^long offset ^long epoch default])
  (getIndexed [ks ^long offset default])
  (getArray [ks ^long offset default])
  (getString [ks ^long offset default]))

(declare ^:private node)

(defmacro ^:private get*
  [[ks offset default] get-method matching-prefix ks-size ks-nth]
  `(let [idx# (~matching-prefix ~offset ~ks ~'prefix)]
      (if (p/== (p/- idx# ~offset) (len ~'prefix))
        (if (p/== idx# ~ks-size)
          (if (identical? ~'value none)
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
          (node nil none ~epoch
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
            (node (drop* ~offset ~ks) ~v ~epoch
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
            none
            ~epoch
            (radix-map
              (~ks-nth ~ks idx#)
              (node (~drop-method (p/inc idx#) ~ks) ~v ~epoch nil)

              (aget ~'prefix (p/- idx# ~offset))
              (node (drop-array (p/inc (p/- idx# ~offset)) ~'prefix) ~'value ~epoch ~'children))))))))

(defmacro ^:private dissoc*
  [[this ks offset epoch default] dissoc-method matching-prefix ks-size ks-nth]
  `(let [idx# (~matching-prefix ~offset ~ks ~'prefix)
         cnt# ~ks-size]
     (if (p/== (p/- idx# ~offset) (Array/getLength ~'prefix))
       (if (p/== idx# cnt#)
         (if (nil? ~'children)
           nil
           (node ~'prefix none ~epoch ~'children))

         (let [x# (~ks-nth ~ks idx#)]
           (if-let [^IRadixNode child# (lookup ~'children x# nil)]
             (node ~'prefix ~'value ~epoch
               (if-let [child'# (~dissoc-method child# ~ks (p/inc idx#) ~epoch)]
                 (put ~'children x# child'#)
                 (remove ~'children x#)))
             ~this)))
       ~this)))

(defmacro ^:private update*
  [[this ks offset epoch f] assoc-method update-method matching-prefix ks-size ks-nth]
  `(let [idx# (~matching-prefix ~offset ~ks ~'prefix)
         cnt# ~ks-size]
     (if (p/== (p/- idx# ~offset) (Array/getLength ~'prefix))
       (if (p/== idx# cnt#)
         (node ~'prefix
           (~f (if (identical? none ~'value) nil ~'value))
           ~epoch
           ~'children)

         (let [x# (~ks-nth ~ks idx#)]
           (if-let [^IRadixNode child# (lookup ~'children x# nil)]
             (node ~'prefix ~'value ~epoch
               (put ~'children x# (~update-method child# ~ks (p/inc idx#) ~epoch ~f)))
             (~assoc-method ~this ~ks ~offset ~epoch (~f nil)))))
       (~assoc-method ~this ~ks ~offset ~epoch (~f nil)))))

(deftype RadixNode
  [^objects prefix
   ^:volatile-mutable value
   ^long epoch
   ^IRadixMap children]
  IRadixNode

  (epoch [_]
    epoch)

  (entries [_ prefix']
    (let [prefix' (into prefix' prefix)
          ks (seq
               (mapcat
                 (fn [k]
                   (.entries
                     ^IRadixNode (lookup children k nil)
                     (conj prefix' k)))
                 (when children (.keys children))))]
      (if (identical? none value)
        ks
        (conj ks (clojure.lang.MapEntry. prefix' value)))))

  (assoc [this ks ^long offset ^long epoch' v]
    (assoc* [this ^clojure.lang.Indexed ks offset epoch' v]
      .assoc
      drop-indexed
      matching-prefix-indexed
      (p/long (.count ^clojure.lang.Indexed ks))
      .nth))

  (dissoc [this ks ^long offset ^long epoch']
    (dissoc* [this ^clojure.lang.Indexed ks offset epoch']
      .dissoc
      matching-prefix-indexed
      (p/long (.count ^clojure.lang.Indexed ks))
      .nth))

  (update [this ks ^long offset ^long epoch' f]
    (update* [this ^clojure.lang.Indexed ks offset epoch' f]
      .assoc
      .update
      matching-prefix-indexed
      (p/long (.count ^clojure.lang.Indexed ks))
      .nth))

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
      char-at))

  clojure.lang.IDeref
  (deref [_]
    {:prefix (seq prefix) :value value :children children}))

(let [empty-ary (object-array [])]
  (defn- node
    ([^long epoch]
       (RadixNode. empty-ary none epoch nil))
    ([prefix ^long epoch children]
       (RadixNode. (if (nil? prefix) empty-ary prefix) none epoch children))
    ([prefix value ^long epoch children]
       (RadixNode. (if (nil? prefix) empty-ary prefix) value epoch children))))


;;;

(declare ->transient)

(defmacro ^:private compile-if [test then else]
  (if (eval test)
    then
    else))

(deftype PersistentRadixTree
  [^RadixNode root
   ^long epoch
   meta]

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
      (reduce
        (fn [acc [k v]]
          (unchecked-add acc (bit-xor (hash k) (hash v))))
        0
        (seq this))))

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
      (PersistentRadixTree. (.assoc root (vec k) 0 epoch' v) epoch' meta)))

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
          (.dissoc root (vec k) 0 epoch')
          (node epoch'))
        epoch'
        meta)))

  clojure.lang.IFn

  (invoke [this k]
    (.valAt this k))

  (invoke [this k default]
    (.valAt this k default)))

(deftype TransientRadixTree
  [^RadixNode root
   ^long epoch
   meta]

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
    (let [root' (.assoc root (vec k) 0 epoch v)]
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
    (let [root' (.dissoc root (vec k) 0 epoch)]
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

(defn radix-tree []
  (PersistentRadixTree. (node 0) 0 nil))
