(ns odoyle.rules
  (:require [clojure.spec.alpha :as s]
            [expound.alpha :as expound]
            [clojure.string :as str])
  #?(:cljs
      (:require-macros [odoyle.rules :refer [ruleset]]))
  (:refer-clojure :exclude [reset! contains?]))

;; parsing

(s/def ::id any?)
(s/def ::attr qualified-keyword?)
(s/def ::value any?)
(s/def ::what-id (s/or :binding symbol? :value ::id))
(s/def ::what-attr (s/or :binding symbol? :value ::attr))
(s/def ::what-value (s/or :binding symbol? :value ::value))
(s/def ::then (s/or :bool boolean? :func #(or (symbol? %) (fn? %))))
(s/def ::what-opts (s/keys :opt-un [::then]))
(s/def ::what-tuple (s/cat :id ::what-id, :attr ::what-attr, :value ::what-value, :opts (s/? ::what-opts)))
(s/def ::what-block (s/cat :header #{:what} :body (s/+ (s/spec ::what-tuple))))
(s/def ::when-block (s/cat :header #{:when} :body (s/+ #(not (keyword? %)))))
(s/def ::then-block (s/cat :header #{:then} :body (s/+ #(not (keyword? %)))))
(s/def ::then-finally-block (s/cat :header #{:then-finally} :body (s/+ #(not (keyword? %)))))

(s/def ::rule (s/cat
                :what-block ::what-block
                :when-block (s/? ::when-block)
                :then-block (s/? ::then-block)
                :then-finally-block (s/? ::then-finally-block)))

(s/def ::rules (s/map-of qualified-keyword? ::rule))

(s/def :odoyle.rules.dynamic-rule/what (s/+ (s/spec ::what-tuple)))
(s/def :odoyle.rules.dynamic-rule/when fn?)
(s/def :odoyle.rules.dynamic-rule/then fn?)
(s/def :odoyle.rules.dynamic-rule/then-finally fn?)
(s/def ::dynamic-rule (s/keys :opt-un [:odoyle.rules.dynamic-rule/what
                                       :odoyle.rules.dynamic-rule/when
                                       :odoyle.rules.dynamic-rule/then
                                       :odoyle.rules.dynamic-rule/then-finally]))

(s/def :odoyle.rules.wrap-rule/what fn?)
(s/def ::wrap-rule (s/keys :opt-un [:odoyle.rules.wrap-rule/what
                                    :odoyle.rules.dynamic-rule/when
                                    :odoyle.rules.dynamic-rule/then
                                    :odoyle.rules.dynamic-rule/then-finally]))
(defn parse [spec content]
  (let [res (s/conform spec content)]
    (if (= ::s/invalid res)
      (throw (ex-info (expound/expound-str spec content) {}))
      res)))

(def ^{:dynamic true
       :doc "Provides the current value of the session from inside a :then or :then-finally block.
This is no longer necessary, because it is accessible via `session` directly."}
  *session* nil)

(def ^{:dynamic true
       :doc "Provides a map of all the matched values from inside a :then block.
This is no longer necessary, because it is accessible via `match` directly."}
  *match* nil)

;; private

(defrecord Fact [id attr value])
(defrecord Token [fact ;; Fact
                  kind ;; :insert, :retract, :update
                  old-fact ;; only used when updating
                  ])
(defrecord Binding [field ;; :id, :attr, or :value
                    sym ;; symbol
                    key ;; keyword
                    ])
(defrecord Match [vars ;; map of binding keywords -> values from facts
                  enabled ;; boolean indicating if this match should be returned in queries
                  ])
(defrecord AlphaNode [path ;; the get-in vector to reach this node from the root
                      test-field ;; :id, :attr, or :value
                      test-value ;; anything
                      children ;; vector of AlphaNode
                      successors ;; vector of JoinNode ids
                      facts ;; map of id -> (map of attr -> Fact)
                      ])
(defrecord MemoryNode [id
                       parent-id ;; JoinNode id
                       child-id ;; JoinNode id
                       leaf-node-id ;; id of the MemoryNode at the end (same as id if this is the leaf node)
                       condition ;; Condition associated with this node
                       matches ;; map of id+attrs -> Match
                       old-matches ;; map of id+attrs -> Match
                       what-fn ;; fn
                       when-fn ;; fn
                       then-fn ;; fn
                       then-finally-fn ;; fn
                       trigger ;; boolean indicating that the :then block can be triggered
                       indexed ;; boolean indicating that its matches should be indexed by vars
                       ])
(defrecord JoinNode [id
                     parent-id ;; MemoryNode id
                     child-id ;; MemoryNode id
                     alpha-node-path ;; the get-in vector to reach the parent AlphaNode from the root
                     condition ;; Condition associated with this node
                     id-key ;; the name of the id binding if we know it
                     old-id-attrs ;; a set of id+attr so the node can keep track of which facts are "new"
                     disable-fast-updates ;; boolean indicating it isn't safe to do fast updates
                     ])
(defrecord Condition [nodes ;; vector of AlphaNode
                      bindings ;; vector of Binding
                      opts ;; map of options
                      ])
(defrecord Rule [name ;; keyword
                 conditions ;; vector of Condition
                 what-fn ;; fn
                 when-fn ;; fn
                 then-fn ;; fn
                 then-finally-fn ;; fn
                 ])
(defrecord Session [alpha-node ;; AlphaNode
                    beta-nodes ;; map of int -> MemoryNode or JoinNode
                    last-id ;; last id assigned to a beta node
                    rule-name->node-id ;; map of rule name -> the id of the associated MemoryNode
                    node-id->rule-name ;; map of the id of a MemoryNode -> the associated rule name
                    id-attr-nodes ;; map of id+attr -> set of alpha node paths
                    then-queue ;; map of node-id -> id+attrs -> execution data
                    then-finally-queue ;; map of node-id -> id+attrs -> execution data
                    make-id+attr ;; function, default is `vector`
                    make-id+attrs ;; function, default is `vector`
                    ])

(defn execute-queue []
  {})
(defn remove-node-from-execute-queue [queue node-id]
  #_(into [] (remove (fn [execution](= (:node-id execution) node-id)))
          queue)
  (dissoc queue node-id)
  #_(reduce (fn [s execution]
              (if (= (:node-id execution) node-id)
                (reduced (disj s execution)) s))
            queue
            queue))
(defn update-execute-queue [queue node-id id+attrs data]
  (update queue node-id assoc id+attrs data))



(defn- add-to-condition [condition field [kind value]]
  (case kind
    :binding (update condition :bindings conj (->Binding field (list 'quote value) (keyword value)))
    :value (update condition :nodes conj (map->AlphaNode {:path nil
                                                          :test-field field
                                                          :test-value value
                                                          :children []
                                                          :successors []
                                                          :facts {}}))))

(defn- ->condition [{:keys [id attr value opts]}]
  (-> {:bindings [] :nodes [] :opts opts}
      (add-to-condition :id id)
      (add-to-condition :attr attr)
      (add-to-condition :value value)))

(defn ->rule
  "Returns a new rule. In most cases, you should use the `ruleset` macro to define rules,
  but if you want to define rules dynamically, you can use this function instead.
  See the README section \"Defining rules dynamically\".
  The one-argument arity is only meant for internal use."
  ([rule-name rule]
   (when (vector? rule)
     (throw (ex-info "The syntax for dynamic rules changed! It now should be a map, and the fns take an extra `session` arg. See the README for more." {})))
   (let [parsed-rule (parse ::dynamic-rule rule)
         parsed-rule (cond-> {:what-block {:body (:what parsed-rule)}}
                             (:when parsed-rule)
                             (assoc-in [:when-block :body] (:when parsed-rule))
                             (:then parsed-rule)
                             (assoc-in [:then-block :body] (:then parsed-rule))
                             (:then-finally parsed-rule)
                             (assoc-in [:then-finally-block :body] (:then-finally parsed-rule)))
         {:keys [rule-name conditions when-body then-body then-finally-body]} (->rule [rule-name parsed-rule])]
     (->Rule rule-name (mapv map->Condition conditions) nil when-body then-body then-finally-body)))
  ([[rule-name parsed-rule]]
   (let [{:keys [what-block when-block then-block then-finally-block]} parsed-rule
         conditions (mapv ->condition (:body what-block))
         when-body (:body when-block)
         then-body (:body then-block)
         then-finally-body (:body then-finally-block)
         syms (->> conditions
                   (mapcat :bindings)
                   (map :sym)
                   (map last) ;; must do this because we quoted it above
                   (filter simple-symbol?) ;; exclude qualified bindings from destructuring
                   set
                   vec)]
     {:rule-name rule-name
      :fn-name (-> (str (namespace rule-name) "-" (name rule-name))
                   (str/replace "." "-")
                   symbol)
      :conditions conditions
      :arg {:keys syms :as 'match}
      :when-body (cond
                   (fn? when-body) when-body
                   (> (count when-body) 1) (cons 'and when-body)
                   :else (first when-body))
      :then-body then-body
      :then-finally-body then-finally-body})))

(defn- add-alpha-node [node new-nodes *alpha-node-path]
  (let [[new-node & other-nodes] new-nodes]
    (if new-node
      (if-let [i (->> (:children node)
                      (map-indexed vector)
                      (some (fn [[i child]]
                              (when (= (select-keys child [:test-field :test-value])
                                       (select-keys new-node [:test-field :test-value]))
                                i))))]
        (do
          (vswap! *alpha-node-path conj :children i)
          (update node :children update i add-alpha-node other-nodes *alpha-node-path))
        (let [path (vswap! *alpha-node-path conj :children (-> node :children count))
              new-node (assoc new-node :path path)]
          (update node :children conj (add-alpha-node new-node other-nodes *alpha-node-path))))
      node)))

(defn- is-ancestor [session node-id1 node-id2]
  (loop [node-id node-id2]
    (if-let [parent-id (:parent-id (get-in session [:beta-nodes node-id]))]
      (if (= node-id1 parent-id)
        1
        (recur parent-id))
      -1)))

(defn- add-condition [[session acc] condition]
  (let [*alpha-node-path (volatile! [:alpha-node])
        session (update session :alpha-node add-alpha-node (:nodes condition) *alpha-node-path)
        alpha-node-path @*alpha-node-path
        *last-id (volatile! (:last-id session))
        join-node-id (vswap! *last-id inc)
        mem-node-id (vswap! *last-id inc)
        parent-mem-node-id (-> acc :mem-node-ids peek)

        acc (-> acc
                (update :mem-node-ids (fn [node-ids]
                                        (if node-ids
                                          (conj node-ids mem-node-id)
                                          [mem-node-id])))
                (update :join-node-ids (fn [node-ids]
                                         (if node-ids
                                           (conj node-ids join-node-id)
                                           [join-node-id])))
                (update :bindings (fn [bindings]
                                    (reduce
                                     (fn [bindings k]
                                       (if (clojure.core/contains? (:all bindings) k)
                                         (update bindings :joins conj k)
                                         (update bindings :all conj k)))
                                     (or bindings
                                         {:all #{} :joins #{}})
                                     (->> condition :bindings (map :key))))))
        
        mem-node (map->MemoryNode {:id mem-node-id
                                   :parent-id join-node-id
                                   :child-id nil
                                   :leaf-node-id nil
                                   :condition condition
                                   :matches {}
                                   :trigger false
                                   :indexed (-> acc :bindings :joins seq boolean)})
        join-node (map->JoinNode {:id join-node-id
                                  :parent-id parent-mem-node-id
                                  :child-id mem-node-id
                                  :alpha-node-path alpha-node-path
                                  :condition condition
                                  :id-key nil
                                  :old-id-attrs #{}
                                  :disable-fast-updates false})
        session (-> session
                    (assoc-in [:beta-nodes join-node-id] join-node)
                    (assoc-in [:beta-nodes mem-node-id] mem-node))
        successor-ids (conj (:successors (get-in session alpha-node-path))
                            join-node-id)
        ;; successors must be sorted by ancestry (descendents first) to avoid duplicate rule firings
        successor-ids (vec (sort (partial is-ancestor session) successor-ids))]
    [(-> session
        (update-in alpha-node-path assoc :successors successor-ids)
        (cond-> parent-mem-node-id
                (assoc-in [:beta-nodes parent-mem-node-id :child-id] join-node-id))
         (assoc :last-id @*last-id))
     acc]))

(def missing ::missing)
(defn- get-vars-from-fact
  "Binds vars from fact, or returns nil to act as a filter for
  skipping beta memory activation"
  [vars bindings fact]
  (reduce
    (fn [m cond-var]
     (let [var-key (:key cond-var)
           x (var-key m missing)
           missing? (identical? missing x)
           fact-val (case (:field cond-var)
                      :id (:id fact)
                      :attr (:attr fact)
                      :value (:value fact))]
       (if (or missing? (= x fact-val))
         (assoc m var-key fact-val)
         (reduced nil))))
    vars
   bindings))

(defn- get-id-attr [session m]
  ((:make-id+attr session) (:id m) (:attr m)))

(declare left-activate-memory-node)

(defn- left-activate-join-node
  ([session node-id id+attrs vars token]
   (let [join-node (get-in session [:beta-nodes node-id])
         alpha-node (get-in session (:alpha-node-path join-node))
         facts (:facts alpha-node)]
     ;; SHORTCUT: if we know the id, only loop over alpha facts with that id
     (if-let [id (some->> (:id-key join-node) (get vars))]
       (reduce-kv
        (fn [session _ alpha-fact]
          (left-activate-join-node session join-node id+attrs vars token alpha-fact))
        session
        (get facts id))
       (reduce-kv
        (fn [session _ attr->fact]
          (reduce-kv
           (fn [session _ alpha-fact]
             (left-activate-join-node session join-node id+attrs vars token alpha-fact))
           session
           attr->fact))
        session
        facts))))
  ([session join-node id+attrs vars token alpha-fact]
   (if-let [new-vars (get-vars-from-fact vars (-> join-node :condition :bindings) alpha-fact)]
     (let [id+attr (get-id-attr session alpha-fact)
           id+attrs (conj id+attrs id+attr)
           new-token (->Token alpha-fact (:kind token) nil)
           new? (not (clojure.core/contains? (:old-id-attrs join-node) id+attr))]
       (left-activate-memory-node session (:child-id join-node) id+attrs new-vars new-token new?))
     session)))

(def ^:private ^:dynamic *triggered-node-ids* nil)

#_#_(defn fast-set [] #?(:clj #{} :cljs (js/Array.)))
(defn conj-set [acc n]
  #?(:clj (conj acc n)
     :cljs (do (.push ^js acc n) acc)))

(defn- left-activate-memory-node [session node-id id+attrs vars token new?]
  (let [kind (:kind token)
        node-path [:beta-nodes node-id]
        node (get-in session node-path)
        ;; if this insert/update fact is new
        ;; and the condition doesn't have {:then false}
        ;; let the leaf node trigger
        session (if (and new?
                         (case kind
                           (:insert :update)
                         (if-let [what-fn (:what-fn node)]
                           ;; if a what fn was supplied via `wrap-rule`,
                           ;; run it so this fact insertion can be intercepted
                           (what-fn (if-let [[then-type then] (-> node :condition :opts :then)]
                                      (case then-type
                                        :bool (fn [session new-fact old-fact]
                                                then)
                                        :func (if-let [old-fact (:old-fact token)]
                                                (fn [session new-fact old-fact]
                                                  (then (:value new-fact) (:value old-fact)))
                                                (fn [session new-fact old-fact]
                                                  true)))
                                      (fn [session new-fact old-fact]
                                        true))
                                    session
                                    (:fact token)
                                    (:old-fact token))
                           ;; otherwise, just check the :then option to determine if this fact
                           ;; can trigger the rule
                           (if-let [[then-type then] (-> node :condition :opts :then)]
                             (case then-type
                               :bool then
                               :func (if-let [old-fact (:old-fact token)]
                                       (then (-> token :fact :value) (:value old-fact))
                                       true))
                               true))
                           false))
                  (do
                    (when *triggered-node-ids* ;; this is only used to improve errors. see `fire-rules`
                      (vswap! *triggered-node-ids* conj (:leaf-node-id node)))
                    (assoc-in session [:beta-nodes (:leaf-node-id node) :trigger] true))
                  session)
        node (get-in session node-path) ;; get node again since trigger may have updated
        leaf-node? (identical? (:id node) (:leaf-node-id node))
        ;; whether the matches in this node should
        ;; return in query results
        enabled? (boolean
                  (or (not leaf-node?)
                      (nil? (:when-fn node))
                      (binding [*session* session
                                *match* vars]
                        ((:when-fn node) session vars))))
        indexed? (:indexed node)
        ;; the id+attr of this token is the last one in the vector
        id+attr (peek id+attrs)
        ;; update session
        session (case kind
                  (:insert :update)
                  (let [match (->Match vars enabled?)]
                    #_(prn "NODE IS INDEXED?"indexed? vars)
                  (as-> session $
                    (if (and leaf-node? (:trigger node) (:then-fn node))
                      (-> $
                          (update-in node-path assoc-in [:old-matches id+attrs]
                                     (-> session
                                         (get-in node-path)
                                                   (get-in [:matches id+attrs])))
                          (update :then-queue update-execute-queue node-id id+attrs nil))
                      $)
                    (update-in $ node-path assoc-in [:matches id+attrs]
                                 match)
                      (if indexed?
                      (reduce-kv
                       (fn [$ k v]
                         (update-in $ node-path assoc-in [:indexed-matches k v id+attrs]
                                    match))
                       $
                       vars)
                        $)
                    (update-in $ [:beta-nodes (:parent-id node) :old-id-attrs]
                                 conj id+attr)))
                  :retract
                  (as-> session $
                    (if (and leaf-node? (:then-finally-fn node))
                      (-> $
                          (update-in node-path assoc-in [:old-matches id+attrs]
                                     (-> session
                                         (get-in node-path)
                                         (get-in [:matches id+attrs])))
                          (update :then-finally-queue update-execute-queue node-id id+attrs nil))
                      $)
                    (update-in $ node-path update :matches dissoc id+attrs)
                    
                    (if indexed?
                    (reduce-kv
                     (fn [$ k v]
                       (update-in $ node-path update-in [:indexed-matches k v]
                                  dissoc  id+attrs))
                     $
                     vars)
                      $)
                    
                    (update-in $ [:beta-nodes (:parent-id node) :old-id-attrs]
                               disj id+attr)))]
    (if-let [join-node-id (:child-id node)]
      (left-activate-join-node session join-node-id id+attrs vars token)
      session)))

(defn- right-activate-join-node [session node-id id+attr token]
  (let [join-node (get-in session [:beta-nodes node-id])
        parent-id (:parent-id join-node)
        parent (get-in session [:beta-nodes parent-id])
        all-matches (:matches parent)
        bindings (-> join-node :condition :bindings)
        fact (:fact token)
        id-key (:id-key join-node)
        child-id (:child-id join-node)]
    (if parent-id
      (if (zero? (count all-matches))
        session
        (if-let [indexed-matches
                 (and (:indexed parent)
                 (not-empty
                  (reduce-kv
                   (fn [acc k v] (merge acc (get-in session [:beta-nodes parent-id :indexed-matches k v])))
                   {}
                        (get-vars-from-fact {} bindings fact))))]
          (do
            #_(prn "INDEX SAVED"(count indexed-matches) "/"(count all-matches))
          (reduce-kv
           (fn [session id+attrs existing-match]
             (if-let [vars (get-vars-from-fact (:vars existing-match) bindings fact)]
               (left-activate-memory-node session child-id (conj id+attrs id+attr) vars token true)
               session))
             session indexed-matches))
          ;; missed index
      (reduce-kv
       (fn [session id+attrs existing-match]
         ;; SHORTCUT: if we know the id, compare it with the token right away
         (let [vars (:vars existing-match)]
           (if (and id-key (some-> (id-key vars) (not= (:id fact))))
           session
             (if-let [vars (get-vars-from-fact vars bindings fact)]
             (left-activate-memory-node session child-id (conj id+attrs id+attr) vars token true)
               session))))
           session all-matches)))
      ;; root node
      (if-let [vars (get-vars-from-fact {} bindings fact)]
        (left-activate-memory-node session child-id ((:make-id+attrs session) id+attr) vars token true)
        session))))

(defn- right-activate-alpha-node [session node-path token]
  (let [fact (:fact token)
        kind (:kind token)
        old-fact (:old-fact token)
        [id attr :as id+attr] (get-id-attr session fact)]
    (as-> session $
      (case kind
        :insert
        (-> $
            (update-in node-path assoc-in [:facts id attr] fact)
            (update-in [:id-attr-nodes id+attr]
                       (fn [node-paths]
                         (let [node-paths (or node-paths #{})]
                           (assert (not (clojure.core/contains? node-paths node-path)))
                           (conj node-paths node-path)))))
        :retract
        (-> $
            (update-in node-path update-in [:facts id] dissoc attr)
            (update :id-attr-nodes
                    (fn [nodes]
                      (let [node-paths (get nodes id+attr)
                            _ (assert (clojure.core/contains? node-paths node-path))
                            node-paths (disj node-paths node-path)]
                        (if (seq node-paths)
                          (assoc nodes id+attr node-paths)
                          (dissoc nodes id+attr))))))
        :update
        (-> $
            (update-in node-path update-in [:facts id attr]
                       (fn [existing-old-fact]
                         (assert (= old-fact existing-old-fact))
                         fact))))
      (reduce
       (fn [session child-id]
         (if (case kind
               :update (-> session :beta-nodes (get child-id) :disable-fast-updates)
               false)
           (-> session
               (right-activate-join-node child-id id+attr (->Token old-fact :retract nil))
               (right-activate-join-node child-id id+attr (->Token fact :insert old-fact)))
           (right-activate-join-node session child-id id+attr token)))
       $
       (:successors (get-in session node-path))))))

(defn- get-alpha-nodes-for-fact [session alpha-node id attr value root?]
  (if root?
    (persistent!
    (reduce
     (fn [nodes child]
        (reduce conj! nodes (get-alpha-nodes-for-fact session child id attr value false)))
     ;; if the root node has successors, that means
     ;; at least one condition had binding symbols
     ;; in all three columns. in that case, add the
     ;; root node to the nodes we are returning,
     ;; because all incoming facts must go through it.
     (if (seq (:successors alpha-node))
        (transient #{(:path alpha-node)})
        (transient #{}))
      (:children alpha-node)))
    (let [test-value (case (:test-field alpha-node)
                       :id id
                       :attr attr
                       :value value)]
      (when (= test-value (:test-value alpha-node))
        (persistent!
        (reduce
         (fn [nodes child]
            (reduce conj! nodes (get-alpha-nodes-for-fact session child id attr value false)))
          (transient #{(:path alpha-node)})
          (:children alpha-node)))))))

(defn- upsert-fact [session id attr value node-paths]
  (let [id+attr ((:make-id+attr session) id attr)
        fact (->Fact id attr value)]
    (if-let [existing-node-paths (get-in session [:id-attr-nodes id+attr])]
      (as-> session $
        ;; retract any facts from nodes that the new fact wasn't inserted in
        (reduce
         (fn [session node-path]
           (if (not (clojure.core/contains? node-paths node-path))
             (let [node (get-in session node-path)
                   old-fact (get-in node [:facts id attr])]
               (assert old-fact "retract")
               (right-activate-alpha-node session node-path (->Token old-fact :retract nil)))
             session))
         $
         existing-node-paths)
        ;; update or insert facts, depending on whether the node already exists
        (reduce
         (fn [session node-path]
           (if (clojure.core/contains? existing-node-paths node-path)
             (let [node (get-in session node-path)
                   old-fact (get-in node [:facts id attr])]
               (assert old-fact "update")
               (right-activate-alpha-node session node-path (->Token fact :update old-fact)))
             (right-activate-alpha-node session node-path (->Token fact :insert nil))))
         $
         node-paths))
      (reduce
       (fn [session node-path]
         (right-activate-alpha-node session node-path (->Token fact :insert nil)))
       session
       node-paths))))

(defn- throw-recursion-limit [session limit executed-nodes]
  (let [;; make a hierarchical map of rule executions
        trigger-map (reduce
                     (fn [m node-id->triggered-node-ids]
                       (reduce-kv
                        (fn [m2 node-id triggered-node-ids]
                          (assoc m2 ((:node-id->rule-name session) node-id)
                                 (reduce
                                  (fn [m3 triggered-node-id]
                                    (let [rule-name ((:node-id->rule-name session) triggered-node-id)]
                                      (assoc m3 rule-name (get m rule-name))))
                                  {}
                                  triggered-node-ids)))
                        {}
                        node-id->triggered-node-ids))
                     {}
                     (reverse executed-nodes))
        ;; find all rules that execute themselves (directly or indirectly)
        find-cycles (fn find-cycles [cycles [k v] cyc]
                      (if (clojure.core/contains? (set cyc) k)
                        (conj cycles (vec (drop-while #(not= % k) (conj cyc k))))
                        (reduce
                         (fn [cycles pair]
                           (find-cycles cycles pair (conj cyc k)))
                         cycles
                         v)))
        cycles (reduce
                (fn [cycles pair]
                  (find-cycles cycles pair []))
                #{}
                trigger-map)]
    (throw (ex-info (str "Recursion limit hit." \newline
                         "This may be an infinite loop." \newline
                         "The current recursion limit is " limit " (set by the :recursion-limit option of fire-rules)." \newline
                         (reduce
                          (fn [s cyc]
                            (str s "Cycle detected! "
                                 (if (= 2 (count cyc))
                                   (str (first cyc) " is triggering itself.")
                                   (str/join " -> " cyc))
                                 \newline))
                          \newline
                          cycles)
                         \newline "Try using {:then false} to prevent triggering rules in an infinite loop.")
                    {}))))

(def ^:private ^:dynamic *recur-countdown* nil)
(def ^:private ^:dynamic *executed-nodes* nil)

;; public

(s/def ::recursion-limit (s/nilable nat-int?))

(s/fdef fire-rules
  :args (s/cat :session ::session
               :opts (s/? (s/keys :opt-un [::recursion-limit]))))
(defn fire-rules
  "Fires :then and :then-finally blocks for any rules whose matches have been updated.
  The opts map may contain:
  
  :recursion-limit  -  Throws an error if rules recursively trigger this many times.
                       The default is 16. Pass nil to disable the limit entirely."
  ([session]
   (fire-rules session {}))
  ([session opts]
   (let [session (if-let [transform (:transform opts)]
                   (transform session)
                   session)
         then-queue (:then-queue session)
         then-finally-queue (:then-finally-queue session)]

     (if (and (or (seq then-queue) (seq then-finally-queue))
              ;; don't fire while inside a rule
              (nil? *session*))
       (let [;; make an fn that will save which rules are triggered by the rules we're about to fire.
             ;; this will be useful for making a nice error message if an infinite loop happens.
             *node-id->triggered-node-ids (volatile! {})
             execute-fn (fn [f node-id]
                          (binding [*triggered-node-ids* (volatile! #{})]
                            (f)
                            (vswap! *node-id->triggered-node-ids update node-id #(into (or % #{}) @*triggered-node-ids*))))
             ;; reset state
             session (assoc session :then-queue (execute-queue) :then-finally-queue (execute-queue))
             untrigger-fn (fn [session node-id _]
                            (update-in session [:beta-nodes node-id] assoc :trigger false))
             session (reduce-kv untrigger-fn session then-queue)
             session (reduce-kv untrigger-fn session then-finally-queue)
             ;; keep a copy of the beta nodes before executing the :then functions.
             ;; if we pull the beta nodes from inside the reduce fn below,
             ;; it'll produce non-deterministic results because `matches`
             ;; could be modified by the reduce itself. see test: non-deterministic-behavior
             beta-nodes (:beta-nodes session)]
         
             ;; execute :then functions
         (reduce-kv
                      (fn [session node-id executions]
                        (reduce-kv
             (fn [session id+attrs _]
                           (let [memory-node (get beta-nodes node-id)
                     old-match (-> memory-node :old-matches (get id+attrs))
                              matches (:matches memory-node)
                                 then-fn  (:then-fn memory-node)]
                 (when-let [{:keys [vars enabled]} (get matches id+attrs)]
                   (when enabled (execute-fn #(then-fn session (with-meta vars
                                                                 {::old-match (:vars old-match)
                                                                  ::id+attrs id+attrs})) node-id)))))
                      session
                         executions))
                      session
                      then-queue)
         
             ;; execute :then-finally functions
         (reduce-kv
                      (fn [session node-id executions]
                        (reduce-kv
             (fn [session id+attrs _]
               (let [memory-node (get beta-nodes node-id)
                     old-vars (-> memory-node :old-matches (get id+attrs) :vars)
                              then-finally-fn (:then-finally-fn memory-node)]
                 (execute-fn #(then-finally-fn session (with-meta old-vars {::id+attrs id+attrs})) node-id)))
                      session
                         executions))
                      session
          then-finally-queue)
         
         ;; recur because there may be new blocks to execute
         (if-let [limit (get opts :recursion-limit 16)]
           (if (= 0 *recur-countdown*)
             (throw-recursion-limit session limit *executed-nodes*)
             (binding [*recur-countdown* (if (nil? *recur-countdown*)
                                           limit
                                           (dec *recur-countdown*))
                       *executed-nodes* (conj (or *executed-nodes* [])
                                              @*node-id->triggered-node-ids)]
               (fire-rules session opts)))
           (fire-rules session opts)))
       session))))

(s/fdef add-rule
  :args (s/cat :session ::session
               :rule #(instance? Rule %)))

(defn add-rule
  "Adds a rule to the given session."
  [session rule]
  (when (get-in session [:rule-name->node-id (:name rule)])
    (throw (ex-info (str (:name rule) " already exists in session") {})))
  (let [conditions (:conditions rule)
        [session {:keys [mem-node-ids join-node-ids bindings]}]
        (reduce add-condition [session {}] conditions)
        
        leaf-node-id (peek mem-node-ids)
        ;; the bindings (symbols) from the :what block
        ;; update all memory nodes with
        ;; the id of their leaf node
        session (reduce (fn [session mem-node-id]
                          (update-in session [:beta-nodes mem-node-id]
                                     (fn [mem-node]
                                       (assoc mem-node :leaf-node-id leaf-node-id, :what-fn (:what-fn rule)))))
                        session
                        mem-node-ids)
        ;; update all join nodes with:
        ;; 1. the name of the id binding, if it exists
        ;; 2. whether to disable fast updates
        session (reduce (fn [session join-node-id]
                          (update-in session [:beta-nodes join-node-id]
                                     (fn [join-node]
                                       (let [joined-key (some (fn [{:keys [field key]}]
                                                                (when (= :value field)
                                                                  key))
                                                              (-> join-node :condition :bindings))
                                             disable-fast-updates (clojure.core/contains?
                                                                   (:joins bindings)
                                                                   joined-key)]
                                         (when (and disable-fast-updates
                                                    (-> (get-in session [:beta-nodes (:child-id join-node)])
                                                        :condition :opts :then first (= :func)))
                                           (throw (ex-info (str "In " (:name rule) " you are making a join with the symbol `" (symbol joined-key) "`, "
                                                                "and passing a custom function in the {:then ...} option. This is not allowed due to "
                                                                "how the implementation works. Luckily, it's easy to fix! Get rid of this join in your :what "
                                                                "block by giving the symbol a different name, such as `" (symbol (str (name joined-key) 2)) "`, "
                                                                "and then enforce the join in your :when block like this: " (list '= (symbol joined-key)
                                                                                                                                  (symbol (str (name joined-key) 2))))
                                                           {})))
                                         (assoc join-node
                                                :id-key (some (fn [{:keys [field key]}]
                                                                (when (and (= :id field)
                                                                           (clojure.core/contains? (:joins bindings) key))
                                                                  key))
                                                              (-> join-node :condition :bindings))
                                                ;; disable fast updates for facts whose value is part of a join
                                                :disable-fast-updates disable-fast-updates)))))
                        session
                        join-node-ids)]
    (-> session
        (assoc-in [:beta-nodes leaf-node-id :when-fn] (:when-fn rule))
        (assoc-in [:beta-nodes leaf-node-id :then-fn] (:then-fn rule))
        (assoc-in [:beta-nodes leaf-node-id :then-finally-fn] (:then-finally-fn rule))
        (assoc-in [:rule-name->node-id (:name rule)] leaf-node-id)
        (assoc-in [:node-id->rule-name leaf-node-id] (:name rule)))))

(s/fdef remove-rule
  :args (s/cat :session ::session
               :rule-name qualified-keyword?))

(defn remove-rule
  "Removes a rule from the given session."
  [session rule-name]
  (if-let [node-id (get-in session [:rule-name->node-id rule-name])]
    (-> (loop [session session
               node-id node-id]
          (if node-id
            (let [node (get-in session [:beta-nodes node-id])
                  session (update session :beta-nodes dissoc node-id)]
              (if (instance? JoinNode node)
                (-> session
                    (update-in (:alpha-node-path node)
                               (fn [alpha-node]
                                 (update alpha-node :successors (fn [successors]
                                                                  (vec (remove #(= % node-id) successors))))))
                    (recur (:parent-id node)))
                (recur session (:parent-id node))))
            session))
        (update :rule-name->node-id dissoc rule-name)
        (update :node-id->rule-name dissoc node-id)
        (update :then-queue remove-node-from-execute-queue node-id)
        (update :then-finally-queue remove-node-from-execute-queue node-id))
    
    (throw (ex-info (str rule-name " does not exist in session") {}))))

#?(:clj
   (defmacro ruleset
     "Returns a vector of rules after transforming the given map."
     [rules]
     (reduce
      (fn [v {:keys [rule-name fn-name conditions when-body then-body then-finally-body arg]}]
        (conj v `(->Rule ~rule-name
                         (mapv map->Condition ~conditions)
                         nil
                         ~(when (some? when-body) ;; need some? because it could be `false`
                            `(fn ~fn-name [~'session ~arg] ~when-body))
                         ~(when then-body
                            `(fn ~fn-name [~'session ~arg] ~@then-body))
                         ~(when then-finally-body
                            `(fn ~fn-name [~'session ~arg] ~@then-finally-body)))))
      []
      (mapv ->rule (parse ::rules rules)))))

(defn ->session
  "Returns a new session."
  [& {:as opts}]
  (map->Session
   (merge
   {:alpha-node (map->AlphaNode {:path [:alpha-node]
                                 :test-field nil
                                 :test-value nil
                                 :children []
                                 :successors []
                                 :facts {}})
    :beta-nodes {}
    :last-id -1
    :rule-name->node-id {}
    :node-id->rule-name {}
    :id-attr-nodes {}
     :then-queue (execute-queue)
     :then-finally-queue (execute-queue)
     :make-id+attr vector
     :make-id+attrs vector}
    opts)))

(s/def ::session #(instance? Session %))

(s/def ::insert-args
  (s/or
   :single-combo (s/cat :session ::session
                        :fact (s/tuple ::id ::attr ::value))
   :batch (s/cat :session ::session
                 :id ::id
                 :attr->value (s/map-of ::attr ::value))
   :single (s/cat :session ::session
                  :id ::id
                  :attr ::attr
                  :value ::value)))

(defn- check-insert-spec
  ([[attr value]]
   (check-insert-spec attr value))
  ([attr value]
   (if-let [spec (s/get-spec attr)]
     (when (= ::s/invalid (s/conform spec value))
       (throw (ex-info (str "Error when checking attribute " attr "\n\n"
                            (expound/expound-str spec value))
                       {})))
     (throw (ex-info (str "Couldn't find spec with name " attr \newline
                          "If you don't want o'doyle to require specs for attributes, call" \newline
                          "(clojure.spec.test.alpha/unstrument 'odoyle.rules/insert)" \newline)
                     {})))))

(def ^:private insert-conformer
  (s/conformer
   (fn [[kind args :as parsed-args]]
     (case kind
       :single-combo (check-insert-spec (nth (:fact args) 1) (nth (:fact args) 2))
       :batch (run! check-insert-spec (:attr->value args))
       :single (check-insert-spec (:attr args) (:value args)))
     parsed-args)))

(s/fdef insert
  :args (s/and ::insert-args insert-conformer))

(defn insert
  "Inserts a fact into the session. Can optionally insert multiple facts with the same id.
  
  Note: if the given fact doesn't match at least one rule, it will be discarded."
  ([session [id attr value]]
   (insert session id attr value))
  ([session id attr->value]
   (reduce-kv (fn [session attr value]
                (insert session id attr value))
              session attr->value))
  ([session id attr value]
   (->> (get-alpha-nodes-for-fact session (:alpha-node session) id attr value true)
        (upsert-fact session id attr value))))

(s/fdef retract
  :args (s/cat :session ::session, :id ::id, :attr ::attr))

(defn retract
  "Retracts the fact with the given id + attr combo."
  [session id attr]
  (let [node-paths (get-in session [:id-attr-nodes ((:make-id+attr session) id attr)])]
     (if node-paths
       (reduce
        (fn [session node-path]
          (let [node (get-in session node-path)
                fact (get-in node [:facts id attr])]
            (right-activate-alpha-node session node-path (->Token fact :retract nil))))
        session
        node-paths)
      session)))


(s/fdef query-all
  :args (s/cat :session ::session, :rule-name (s/? qualified-keyword?)))

(defn query-all
  "When called with just a session, returns a vector of all inserted facts.
  Otherwise, returns a vector of maps containing all the matches for the given rule."
  ([session]
   (mapv (fn [[[id attr] nodes]]
           (-> (get-in session (first nodes))
               (get-in [:facts id attr])
               ((juxt :id :attr :value))))
         (:id-attr-nodes session)))
  ([session rule-name]
   (let [rule-id (or (get-in session [:rule-name->node-id rule-name])
                     (throw (ex-info (str rule-name " not in session") {})))
         rule (get-in session [:beta-nodes rule-id])]
     (->Eduction (comp (map val) (filter :enabled) (map :vars))
      (:matches rule)))))


(s/fdef contains?
  :args (s/cat :session ::session, :id ::id, :attr ::attr))

(defn contains?
  "Returns true if the session contains a fact with the given id and attribute."
  [session id attr]
  (clojure.core/contains? (:id-attr-nodes session) ((:make-id+attr session) id attr)))

(s/fdef wrap-rule
  :args (s/cat :rule #(instance? Rule %)
               :rule-fns ::wrap-rule))

(defn wrap-rule
  "Wraps the functions of a rule so they can be conveniently intercepted
  for debugging or other purposes.
  See the README section \"Debugging\"."
  [rule {what-fn :what, when-fn :when, then-fn :then, then-finally-fn :then-finally}]
  (cond-> (assoc rule :what-fn what-fn)
    (and (:when-fn rule) when-fn)
    (update :when-fn
            (fn wrap-when [f]
              (fn [session match]
                (when-fn f session match))))
    (and (:then-fn rule) then-fn)
    (update :then-fn
            (fn wrap-then [f]
              (fn [session match]
                (then-fn f session match))))
    (and (:then-finally-fn rule) then-finally-fn)
    (update :then-finally-fn
            (fn wrap-then-finally [f]
              (fn [session old-match]
                (then-finally-fn f session old-match))))))


