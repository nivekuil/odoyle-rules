(ns odoyle.rules-test
  (:require [clojure.test :refer :all]
            [odoyle.rules :as o]
            [clojure.spec.test.alpha :as st]))

(st/instrument)

(deftest num-of-conditions-not=-num-of-facts
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::num-conds-and-facts
           [:what
            [b ::color "blue"]
            [y ::left-of z]
            [a ::color "maize"]
            [y ::right-of b]
            [x ::height h]
            :then
            (is (= a ::alice))
            (is (= b ::bob))
            (is (= y ::yair))
            (is (= z ::zach))]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::yair ::left-of ::zach)
      (o/insert ::alice ::color "maize")
      (o/insert ::yair ::right-of ::bob)
      (o/insert ::xavier ::height 72)
      (o/insert ::thomas ::height 72)
      (o/insert ::george ::height 72)
      o/fire-rules
      ((fn [session]
         (is (= 3 (count (o/query-all session ::num-conds-and-facts))))
         session))))

(deftest adding-facts-out-of-order
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::out-of-order
           [:what
            [x ::right-of y]
            [y ::left-of z]
            [z ::color "red"]
            [a ::color "maize"]
            [b ::color "blue"]
            [c ::color "green"]
            [d ::color "white"]
            [s ::on "table"]
            [y ::right-of b]
            [a ::left-of d]
            :then
            (is (= a ::alice))
            (is (= b ::bob))
            (is (= y ::yair))
            (is (= z ::zach))]}))
      (o/insert ::xavier ::right-of ::yair)
      (o/insert ::yair ::left-of ::zach)
      (o/insert ::zach ::color "red")
      (o/insert ::alice ::color "maize")
      (o/insert ::bob ::color "blue")
      (o/insert ::charlie ::color "green")
      (o/insert ::seth ::on "table")
      (o/insert ::yair ::right-of ::bob)
      (o/insert ::alice ::left-of ::david)
      (o/insert ::david ::color "white")
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::out-of-order))))
         session))))

(deftest duplicate-facts
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::duplicate-facts
           [:what
            [x ::self y]
            [x ::color c]
            [y ::color c]]}))
      (o/insert ::bob ::self ::bob)
      (o/insert ::bob ::color "red")
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::duplicate-facts))))
         (is (= "red" (:c (o/query session ::duplicate-facts))))
         session))
      (o/insert ::bob ::color "green")
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::duplicate-facts))))
         (is (= "green" (:c (o/query session ::duplicate-facts))))
         session))))

(deftest removing-facts
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::removing-facts
           [:what
            [b ::color "blue"]
            [y ::left-of z]
            [a ::color "maize"]
            [y ::right-of b]]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::yair ::left-of ::zach)
      (o/insert ::alice ::color "maize")
      (o/insert ::yair ::right-of ::bob)
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::removing-facts))))
         session))
      (o/retract ::yair ::right-of)
      ((fn [session]
         (is (= 0 (count (o/query-all session ::removing-facts))))
         session))
      (o/retract ::bob ::color)
      (o/insert ::bob ::color "blue")
      (o/insert ::yair ::right-of ::bob)
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::removing-facts))))
         session))))

(deftest updating-facts
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::updating-facts
           [:what
            [b ::color "blue"]
            [y ::left-of z]
            [a ::color "maize"]
            [y ::right-of b]]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::yair ::left-of ::zach)
      (o/insert ::alice ::color "maize")
      (o/insert ::yair ::right-of ::bob)
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::updating-facts))))
         (is (= ::zach (:z (o/query session ::updating-facts))))
         session))
      (o/insert ::yair ::left-of ::xavier)
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::updating-facts))))
         (is (= ::xavier (:z (o/query session ::updating-facts))))
         session))))

(deftest updating-facts-in-different-alpha-nodes
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::updating-facts-diff-nodes
           [:what
            [b ::color "blue"]
            [y ::left-of ::zach]
            [a ::color "maize"]
            [y ::right-of b]]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::yair ::left-of ::zach)
      (o/insert ::alice ::color "maize")
      (o/insert ::yair ::right-of ::bob)
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::updating-facts-diff-nodes))))
         session))
      (o/insert ::yair ::left-of ::xavier)
      o/fire-rules
      ((fn [session]
         (is (= 0 (count (o/query-all session ::updating-facts-diff-nodes))))
         session))))

(deftest facts-can-be-stored-in-different-alpha-nodes
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::rule1
           [:what
            [a ::left-of ::zach]]
           ::rule2
           [:what
            [a ::left-of z]]}))
      (o/insert ::alice ::left-of ::zach)
      o/fire-rules
      ((fn [session]
         (is (= ::alice (:a (o/query session ::rule1))))
         (is (= ::zach (:z (o/query session ::rule2))))
         session))))

(deftest complex-conditions
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::complex-cond
           [:what
            [b ::color "blue"]
            [y ::left-of z]
            [a ::color "maize"]
            [y ::right-of b]
            :when
            (not= z ::zach)]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::yair ::left-of ::zach)
      (o/insert ::alice ::color "maize")
      (o/insert ::yair ::right-of ::bob)
      o/fire-rules
      ((fn [session]
         (is (= 0 (count (o/query-all session ::complex-cond))))
         session))
      (o/insert ::yair ::left-of ::charlie)
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::complex-cond))))
         session))))

(deftest out-of-order-joins-between-id-and-value
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::rule1
           [:what
            [b ::right-of ::alice]
            [y ::right-of b]
            [b ::color "blue"]]}))
      (o/insert ::bob ::right-of ::alice)
      (o/insert ::bob ::color "blue")
      (o/insert ::yair ::right-of ::bob)
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::rule1))))
         session))))

(deftest simple-conditions
  (let [*count (atom 0)]
    (-> (reduce o/add-rule (o/->session)
          (o/ruleset
            {::simple-cond
             [:what
              [b ::color "blue"]
              :when
              false
              :then
              (swap! *count inc)]}))
        (o/insert ::bob ::color "blue")
        o/fire-rules
        ((fn [session]
           (is (= 0 @*count))
           session)))))

(deftest queries
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::get-person
           [:what
            [id ::color color]
            [id ::left-of left-of]
            [id ::height height]]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::bob ::left-of ::zach)
      (o/insert ::bob ::height 72)
      (o/insert ::alice ::color "green")
      (o/insert ::alice ::left-of ::bob)
      (o/insert ::alice ::height 64)
      (o/insert ::charlie ::color "red")
      (o/insert ::charlie ::left-of ::alice)
      (o/insert ::charlie ::height 72)
      o/fire-rules
      ((fn [session]
         (is (= 3 (count (o/query-all session ::get-person))))
         (is (= ::charlie (:id (o/query session ::get-person))))
         session))))

(deftest creating-a-ruleset
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::bob
           [:what
            [b ::color "blue"]
            [b ::right-of a]
            :then
            (is (= a ::alice))
            (is (= b ::bob))]
           ::alice
           [:what
            [a ::color "red"]
            [a ::left-of b]
            :then
            (is (= a ::alice))
            (is (= b ::bob))]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::bob ::right-of ::alice)
      (o/insert ::alice ::color "red")
      (o/insert ::alice ::left-of ::bob)
      o/fire-rules))

(deftest dont-trigger-rule-when-updating-certain-facts
  (let [*count (atom 0)]
    (-> (reduce o/add-rule (o/->session)
          (o/ruleset
            {::dont-trigger
             [:what
              [b ::color "blue"]
              [a ::color c {:then false}]
              :then
              (swap! *count inc)]}))
        (o/insert ::bob ::color "blue")
        o/fire-rules
        (o/insert ::alice ::color "red")
        o/fire-rules
        (o/insert ::alice ::color "maize")
        o/fire-rules
        ((fn [session]
           (is (= 1 @*count))
           session)))))

(deftest inserting-inside-a-rule
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::rule1
           [:what
            [b ::color "blue"]
            [::alice ::color c {:then false}]
            :then
            (o/insert! ::alice ::color "maize")]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::alice ::color "red")
      o/fire-rules
      ((fn [session]
         (is (= "maize" (:c (o/query session ::rule1))))
         session))))

(deftest inserting-inside-a-rule-can-trigger-more-than-once
  (let [*count (atom 0)]
    (-> (reduce o/add-rule (o/->session)
          (o/ruleset
            {::rule1
             [:what
              [b ::color "blue"]
              :then
              (o/insert! ::alice ::color "maize")
              (o/insert! ::charlie ::color "gold")]
             ::rule2
             [:what
              [::alice ::color c1]
              [other-person ::color c2]
              :when
              (not= other-person ::alice)
              :then
              (swap! *count inc)]}))
        (o/insert ::alice ::color "red")
        (o/insert ::bob ::color "blue")
        o/fire-rules
        ((fn [session]
           (is (= 3 @*count))
           session)))))

(deftest inserting-inside-a-rule-cascades
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::rule1
           [:what
            [b ::color "blue"]
            :then
            (o/insert! ::charlie ::right-of ::bob)]
           ::rule2
           [:what
            [c ::right-of b]
            :then
            (o/insert! b ::left-of c)]
           ::rule3
           [:what
            [b ::left-of c]]}))
      (o/insert ::bob ::color "blue")
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::rule1))))
         (is (= 1 (count (o/query-all session ::rule2))))
         (is (= 1 (count (o/query-all session ::rule3))))
         session))))

(deftest conditions-can-use-external-values
  (let [*allow-rule-to-fire (atom false)]
    (-> (reduce o/add-rule (o/->session)
          (o/ruleset
            {::rule1
             [:what
              [a ::left-of b]
              :when
              @*allow-rule-to-fire]}))
        (o/insert ::alice ::left-of ::zach)
        o/fire-rules
        ((fn [session]
           (reset! *allow-rule-to-fire true)
           session))
        (o/insert ::alice ::left-of ::bob)
        o/fire-rules
        ((fn [session]
           (is (= 1 (count (o/query-all session ::rule1))))
           (reset! *allow-rule-to-fire false)
           session))
        (o/insert ::alice ::left-of ::zach)
        o/fire-rules
        ((fn [session]
           (is (= 0 (count (o/query-all session ::rule1))))
           session)))))

(deftest id+attr-combos-can-be-stored-in-multiple-alpha-nodes
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::get-alice
           [:what
            [::alice ::color color]
            [::alice ::height height]]
           ::get-person
           [:what
            [id ::color color]
            [id ::height height]]}))
      (o/insert ::alice ::color "blue")
      (o/insert ::alice ::height 60)
      o/fire-rules
      ((fn [session]
         (let [alice (o/query session ::get-alice)]
           (is (= "blue" (:color alice)))
           (is (= 60 (:height alice))))
         session))
      (o/retract ::alice ::color)
      ((fn [session]
         (is (= 0 (count (o/query-all session ::get-alice))))
         session))))

(deftest ids-can-be-arbitrary-integers
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::rule1
           [:what
            [b ::color "blue"]
            [y ::left-of z]
            [a ::color "maize"]
            [y ::right-of b]
            [z ::left-of b]
            :then
            (is (= a ::alice))
            (is (= b ::bob))
            (is (= y ::yair))
            (is (= z 1))]}))
      (o/insert ::bob ::color "blue")
      (o/insert ::yair ::left-of 1)
      (o/insert ::alice ::color "maize")
      (o/insert ::yair ::right-of ::bob)
      (o/insert 1 ::left-of ::bob)
      o/fire-rules))

(deftest join-value-with-id
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::rule1
           [:what
            [b ::left-of id]
            [id ::color color]
            [id ::height height]]}))
      (o/insert ::alice ::color "blue")
      (o/insert ::alice ::height 60)
      (o/insert ::charlie ::color "green")
      (o/insert ::charlie ::height 72)
      (o/insert ::bob ::left-of ::alice)
      o/fire-rules
      ((fn [session]
         (is (= 1 (count (o/query-all session ::rule1))))
         session))))

(deftest multiple-joins
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::rule1
           [:what
            [pid ::kind :player]
            [pid ::color pcolor]
            [pid ::height pheight]
            [eid ::kind kind]
            [eid ::color ecolor {:then false}]
            [eid ::height eheight {:then false}]
            :when
            (not= kind :player)
            :then
            (o/insert! eid ::color "green")
            (o/insert! eid ::height 70)]}))
      (o/insert 1 {::kind :player
                   ::color "red"
                   ::height 72})
      (o/insert 2 {::kind :enemy
                   ::color "blue"
                   ::height 60})
      o/fire-rules
      ((fn [session]
         (is (= "green" (:ecolor (o/query session ::rule1))))
         session))))

(deftest join-followed-by-non-join
  (-> (reduce o/add-rule (o/->session)
        (o/ruleset
          {::rule1
           [:what
            [id ::x x]
            [id ::y y]
            [id ::xv xv]
            [id ::yv yv]
            [::bob ::left-of z]]}))
      (o/insert ::bob ::left-of ::zach)
      (o/insert ::alice {::x 0 ::y 0 ::xv 1 ::yv 1})
      (o/insert ::charlie {::x 1 ::y 1 ::xv 0 ::yv 0})
      o/fire-rules
      ((fn [session]
         (is (= 2 (count (o/query-all session ::rule1))))
         session))))

(deftest only-last-condition-can-fire
  (let [*count (atom 0)]
    (-> (reduce o/add-rule (o/->session)
          (o/ruleset
            {::rule1
             [:what
              [id ::left-of ::bob {:then false}]
              [id ::color color {:then false}]
              [::alice ::height height]
              :then
              (swap! *count inc)]}))
        (o/insert ::alice ::height 60) ;; out of order
        (o/insert ::alice ::left-of ::bob)
        (o/insert ::alice ::color "blue")
        o/fire-rules
        ((fn [session]
           (is (= 1 @*count))
           session))
        (o/retract ::alice ::height)
        (o/retract ::alice ::left-of)
        (o/retract ::alice ::color)
        (o/insert ::alice ::height 60)
        (o/insert ::alice ::left-of ::bob)
        (o/insert ::alice ::color "blue")
        o/fire-rules
        ((fn [session]
           (is (= 2 @*count))
           session))
        (o/insert ::alice ::left-of ::bob)
        (o/insert ::alice ::color "blue")
        o/fire-rules
        ((fn [session]
           (is (= 2 @*count))
           session))
        (o/insert ::alice ::height 60)
        o/fire-rules
        ((fn [session]
           (is (= 3 @*count))
           session)))))

