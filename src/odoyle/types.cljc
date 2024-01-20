(ns odoyle.types)

(def make-id+attr
  #?(:default vector
     :cljs (do (deftype IdAttr [id attr ^:mutable ^number _hasheq]
                 Object
                 (toString [coll]
                   (str id ","attr ))
                 IEquiv
                 (-equiv
                   [this that]
                   (boolean
                    (or (identical? this that)
                        (and (instance? IdAttr that)
                             (= id (.-id ^IdAttr that))
                             (= attr (.-attr ^IdAttr that))))))

                 IIndexed
                 (-nth [coll n] (-nth coll n nil))
                 (-nth [coll n not-found] (case n, 0 id, 1 attr, not-found))
                 
                 IHash
                 (-hash [o] 
                   (if (zero? _hasheq)
                     (set! _hasheq (+ (* 31 (hash id)) (hash attr)))
                     _hasheq)))
               (fn [id attr] (->IdAttr id attr 0)))))

(def make-id+attrs
  #?(:default vector
     :cljs (do (deftype IdAttrs [last-object h]
                 IStack
                 (-peek [coll] last-object)
                 ICollection
                 (-conj [coll o]
                   (IdAttrs. o (+ h (* 31 (hash o)))))
                 IEquiv
                 (-equiv
                   [this that]
                   (boolean (or (identical? this that)
                                (and (instance? IdAttrs that)
                                     (= h (.-h ^IdAttrs that))
                                     (= last-object (.-last-object ^IdAttrs that))))))
                 IHash
                 (-hash [o] h))
               (fn [id+attr]
                 (->IdAttrs id+attr (* 31 (hash id+attr)))))))
