(ns circledb.core
  (:require [clojure.set :as CS]))

;; =========
;; Records / Protocols
;; =========

(defrecord Database [layers top-id curr-time])
(defrecord Layer    [storage VAET AVET VEAT EAVT])
(defrecord Entity   [id attrs])
(defrecord Attr     [name value ts prev-ts])

(defprotocol Storage
  (get-entity  [storage e-id])
  (write-entity [storage entity])
  (drop-entity [storage entity]))

;; In-memory storage implemented as a record that behaves like a map.
(defrecord InMemory [] Storage
  (get-entity  [storage e-id] (e-id storage))                     ;; keywords act as fns on maps/records
  (write-entity [storage entity] (assoc storage (:id entity) entity))
  (drop-entity [storage entity] (dissoc storage (:id entity))))

;; =========
;; Constructors / helpers
;; =========

(defn make-entity
  ([]    (make-entity :db/no-id-yet))
  ([id]  (Entity. id {})))

(defn make-attr
  "type is required (e.g. :db/ref). Optional :cardinality defaults to :db/single.
   We store values as sets; singles become one-element sets."
  [name value type & {:keys [cardinality] :or {cardinality :db/single}}]
  {:pre [(contains? #{:db/single :db/multiple} cardinality)]}
  (with-meta (Attr. name value -1 -1)
    {:type type :cardinality cardinality}))

(defn add-attr [ent attr]
  (let [attr-id (keyword (:name attr))]
    (assoc-in ent [:attrs attr-id] attr)))

(defn single? [attr] (= :db/single (:cardinality (meta attr))))
(defn ref?    [attr] (= :db/ref    (:type (meta attr))))
(defn collify [x]    (if (set? x) x #{x}))
(defn always [& _]   true)

;; =========
;; Indexes
;; =========

(defn make-index [from-eav to-eav usage-pred]
  (with-meta {} {:from-eav from-eav :to-eav to-eav :usage-pred usage-pred}))

(defn from-eav   [index] (:from-eav   (meta index)))
(defn to-eav     [index] (:to-eav     (meta index)))
(defn usage-pred [index] (:usage-pred (meta index)))

(defn indexes [] [:VAET :AVET :VEAT :EAVT])

;; =========
;; DB init
;; =========

(defn make-db
  "Returns an Atom (connection) referring to an empty Database value."
  []
  (atom
   (Database.
    [(Layer.
       (->InMemory)                                 ;; storage
       ;; VAET: value -> attr -> entity  (only for refs)
       (make-index #(vector %3 %2 %1) #(vector %3 %2 %1) #(ref? %))
       ;; AVET: attr -> value -> entity
       (make-index #(vector %2 %3 %1) #(vector %3 %1 %2) always)
       ;; VEAT: value -> entity -> attr
       (make-index #(vector %3 %1 %2) #(vector %2 %3 %1) always)
       ;; EAVT: entity -> attr -> value
       (make-index #(vector %1 %2 %3) #(vector %1 %2 %3) always))]
    0                                                  ;; top-id
    0)))                                               ;; curr-time

;; =========
;; Basic accessors (operate on DB values, not the atom)
;; =========

(defn entity-at
  ([db ent-id] (entity-at db (:curr-time db) ent-id))
  ([db ts ent-id] (get-entity (get-in db [:layers ts :storage]) ent-id)))

(defn attr-at
  ([db ent-id attr-name] (attr-at db ent-id attr-name (:curr-time db)))
  ([db ent-id attr-name ts] (get-in (entity-at db ts ent-id) [:attrs attr-name])))

(defn value-of-at
  ([db ent-id attr-name]    (:value (attr-at db ent-id attr-name)))
  ([db ent-id attr-name ts] (:value (attr-at db ent-id attr-name ts))))

(defn indx-at
  ([db kind]    (indx-at db kind (:curr-time db)))
  ([db kind ts] (kind ((:layers db) ts))))

(defn evolution-of [db ent-id attr-name]
  (loop [res [] ts (:curr-time db)]
    (if (= -1 ts)
      (reverse res)
      (let [attr (attr-at db ent-id attr-name ts)]
        (recur (conj res {(:ts attr) (:value attr)}) (:prev-ts attr))))))

;; =========
;; Internal helpers for add / update / remove
;; =========

(defn- next-ts [db] (inc (:curr-time db)))

(defn- update-creation-ts [ent ts-val]
  (reduce #(assoc-in %1 [:attrs %2 :ts] ts-val) ent (keys (:attrs ent))))

(defn- next-id [db ent]
  (let [top-id (:top-id db)
        ent-id (:id ent)
        increased-id (inc top-id)]
    (if (= ent-id :db/no-id-yet)
      [(keyword (str increased-id)) increased-id]
      [ent-id top-id])))

(defn- fix-new-entity [db ent]
  (let [[ent-id next-top-id] (next-id db ent)
        new-ts               (next-ts db)]
    [(update-creation-ts (assoc ent :id ent-id) new-ts) next-top-id]))

(defn- update-entry-in-index
  "Add or remove a single leaf in a 3-deep index path."
  [index path operation]
  (let [update-path (butlast path)
        update-value (last path)
        curr (get-in index update-path #{})]
    (case operation
      :db/remove (assoc-in index update-path (disj curr update-value))
      ;; default add
      (assoc-in index update-path (conj curr update-value)))))

(defn- update-attr-in-index [index ent-id attr-name target-val operation]
  (let [coll-target (collify target-val)
        f (fn [ind v]
            (update-entry-in-index
             ind
             ((from-eav index) ent-id attr-name v)
             operation))]
    (reduce f index coll-target)))

(defn- add-entity-to-index [ent layer ind-name]
  (let [ent-id (:id ent)
        index (ind-name layer)
        all-attrs (vals (:attrs ent))
        relevant-attrs (filter #((usage-pred index) %) all-attrs)
        add-fn (fn [ind attr]
                 (update-attr-in-index ind ent-id (:name attr) (:value attr) :db/add))]
    (assoc layer ind-name (reduce add-fn index relevant-attrs))))

(defn- remove-entity-from-index [ent layer ind-name]
  (let [ent-id (:id ent)
        index (ind-name layer)
        all-attrs (vals (:attrs ent))
        relevant-attrs (filter #((usage-pred index) %) all-attrs)
        rm-fn (fn [ind attr]
                (update-attr-in-index ind ent-id (:name attr) (:value attr) :db/remove))]
    (assoc layer ind-name (reduce rm-fn index relevant-attrs))))

(defn- update-attr-modification-time [attr new-ts]
  (assoc attr :ts new-ts :prev-ts (:ts attr)))

(defn- update-attr-value [attr value operation]
  (cond
    (single? attr)               (assoc attr :value #{value})
    (= :db/reset-to operation)   (assoc attr :value value)
    (= :db/add operation)        (assoc attr :value (CS/union (:value attr) value))
    (= :db/remove operation)     (assoc attr :value (CS/difference (:value attr) value))))

(defn- update-attr [attr new-val new-ts operation]
  {:pre [(if (single? attr)
           (contains? #{:db/reset-to :db/remove} operation)
           (contains? #{:db/reset-to :db/add :db/remove} operation))]}
  (-> attr
      (update-attr-modification-time new-ts)
      (update-attr-value new-val operation)))

(defn- update-layer
  "Replace old attr with updated attr in storage, and refresh all indexes by
   removing old leaves and adding new ones."
  [layer ent-id old-attr new-attr]
  (let [entity        (get-entity (:storage layer) ent-id)
        entity'       (assoc-in entity [:attrs (:name old-attr)] new-attr)
        storage'      (write-entity (:storage layer) entity')
        layer'        (assoc layer :storage storage')]
    (-> layer'
        ;; remove old
        (reduce (fn [ly ind-name]
                  (update ly ind-name
                          #(update-attr-in-index % ent-id (:name old-attr) (:value old-attr) :db/remove)))
                layer')
        ;; add new
        (reduce (fn [ly ind-name]
                  (update ly ind-name
                          #(update-attr-in-index % ent-id (:name new-attr) (:value new-attr) :db/add)))
                (indexes)))))

;; =========
;; Public: add / remove / update (operate on DB values)
;; =========

(defn add-entity [db ent]
  (let [[fixed-ent next-top-id]        (fix-new-entity db ent)
        layer-with-storage             (update-in (last (:layers db)) [:storage] write-entity fixed-ent)
        new-layer                      (reduce (partial add-entity-to-index fixed-ent)
                                              layer-with-storage (indexes))]
    (assoc db :layers (conj (:layers db) new-layer) :top-id next-top-id)))

(defn add-entities [db ents-seq]
  (reduce add-entity db ents-seq))

(defn- reffing-to [e-id layer]
  (let [vaet (:VAET layer)]
    (for [[attr-name reffing-set] (e-id vaet)
          reffing reffing-set]
      [reffing attr-name])))

(defn- remove-back-refs [db e-id layer]
  (let [reffing-datoms (reffing-to e-id layer)
        remove-fn (fn [d [e a]] (update-entity db e a e-id :db/remove))
        clean-db (reduce remove-fn db reffing-datoms)]
    (last (:layers clean-db))))

(defn remove-entity [db ent-id]
  (let [ent           (entity-at db ent-id)
        layer         (remove-back-refs db ent-id (last (:layers db)))
        no-ref-layer  (update-in layer [:VAET] dissoc ent-id)
        no-ent-layer  (assoc no-ref-layer :storage (drop-entity (:storage no-ref-layer) ent))
        new-layer     (reduce (partial remove-entity-from-index ent) no-ent-layer (indexes))]
    (assoc db :layers (conj (:layers db) new-layer))))

(defn update-entity
  "By default performs :db/reset-to. For :db/multiple attrs you can also use :db/add or :db/remove,
   passing sets as new-val (e.g. #{:tag1})."
  ([db ent-id attr-name new-val]
   (update-entity db ent-id attr-name new-val :db/reset-to))
  ([db ent-id attr-name new-val operation]
   (let [update-ts (next-ts db)
         layer (last (:layers db))
         attr (attr-at db ent-id attr-name)
         updated-attr (update-attr attr new-val update-ts operation)
         fully-updated-layer (update-layer layer ent-id attr updated-attr)]
     (update-in db [:layers] conj fully-updated-layer))))

;; =========
;; Transactions (on the Atom ‘connection’) + What-if (on plain DB value)
;; =========

(defn transact-on-db [initial-db ops]
  (loop [[op & rst] ops, transacted initial-db]
    (if op
      (recur rst (apply (first op) transacted (rest op)))
      (let [initial-layer (:layers initial-db)
            new-layer (last (:layers transacted))]
        (assoc initial-db
               :layers   (conj initial-layer new-layer)
               :curr-time (next-ts initial-db)
               :top-id   (:top-id transacted))))))

(defmacro _transact [db op & txs]
  (when txs
    (loop [[fr & rs] txs, res [op db `transact-on-db], acc []]
      (if fr
        (recur rs res (conj acc (vec fr)))
        (list* (conj res acc))))))

(defmacro transact [db-conn & txs] `(_transact ~db-conn swap! ~@txs))
(defn- _what-if [db f txs] (f db txs))
(defmacro what-if   [db & ops]     `(_transact ~db _what-if ~@ops))

;; =========
;; Graph helpers
;; =========

(defn incoming-refs
  ([db ts ent-id & ref-names]
   (let [vaet        (indx-at db :VAET ts)
         all-attr-map (vaet ent-id)
         filtered-map (if (seq ref-names)
                        (select-keys all-attr-map ref-names)  ;; fixed arg order
                        all-attr-map)]
     (reduce into #{} (vals filtered-map)))))

(defn outgoing-refs
  ([db ts ent-id & ref-names]
   (let [val-filter-fn (if (seq ref-names)
                         #(vals (select-keys % ref-names))   ;; fixed arg order
                         vals)]
     (if-not ent-id
       []
       (->> (entity-at db ts ent-id)
            :attrs val-filter-fn (filter ref?) (mapcat :value))))))

;; =========
;; Query engine (small datalog subset, single joining var)
;; =========

(defn variable?
  "Considers strings beginning with '?' as variables. Arity 2 kept for macro uses in the article."
  ([s] (variable? s true))
  ([s _] (and (string? s) (.startsWith ^String s "?"))))

(defmacro symbol-col-to-set [coll] `(set (map str ~coll)))

(defmacro clause-term-expr [clause-term]
  (cond
    ;; variable
    (variable? (str clause-term))              `#(= % %)
    ;; constant
    (not (coll? clause-term))                  `#(= % ~clause-term)
    ;; unary operator, e.g. (pred ?x)
    (= 2 (count clause-term))                  `#(~(first clause-term) %)
    ;; binary operator, var is 1st
    (variable? (str (second clause-term)))     `#(~(first clause-term) % ~(last clause-term))
    ;; binary operator, var is 2nd
    (variable? (str (last clause-term)))       `#(~(first clause-term) ~(second clause-term) %)))

(defmacro clause-term-meta [clause-term]
  (cond
    (coll? clause-term)                        `(first (filter #(variable? % false) (map str ~clause-term)))
    (variable? (str clause-term) false)        (str clause-term)
    :else                                      nil))

(defmacro pred-clause [clause]
  (loop [[trm# & rst#] clause, exprs# [], metas# []]
    (if trm#
      (recur rst# (conj exprs# `(clause-term-expr ~trm#))
             (conj metas#  `(clause-term-meta ~trm#)))
      (with-meta exprs# {:db/variable metas#}))))

(defmacro q-clauses-to-pred-clauses [clauses]
  (loop [[frst# & rst#] clauses, preds# []]
    (if-not frst#
      preds#
      (recur rst# `(conj ~preds# (pred-clause ~frst#))))))

(defn index-of-joining-variable [query-clauses]
  (let [metas-seq   (map #(:db/variable (meta %)) query-clauses)
        collapse-fn (fn [accV v] (map #(when (= %1 %2) %1) accV v))
        collapsed   (reduce collapse-fn metas-seq)]
    (first (keep-indexed #(when (variable? %2 false) %1) collapsed))))

(defn build-query-plan [query]
  (let [term-ind (index-of-joining-variable query)
        ind      (case term-ind
                   0 :AVET
                   1 :VEAT
                   2 :EAVT)]
    (partial (fn single-index-query-plan [query indx db]
               (let [q-res (let [idx (indx-at db indx)]
                             (letfn [(filter-index [index predicate-clauses]
                                       (for [pred-clause predicate-clauses
                                             :let [[lvl1 lvl2 lvl3] (apply (from-eav index) pred-clause)]
                                             [k1 l2map] index :when (try (lvl1 k1) (catch Exception _ false))
                                             [k2  l3set] l2map :when (try (lvl2 k2) (catch Exception _ false))
                                             :let [res (set (filter lvl3 l3set))]]
                                         (with-meta [k1 k2 res] (meta pred-clause))))
                                     (items-that-answer-all-conditions [items-seq num-conds]
                                       (->> items-seq (map vec) (reduce into [])
                                            (frequencies)
                                            (filter #(<= num-conds (last %)))
                                            (map first) (set)))
                                     (mask-path-leaf-with-items [relevant-items path]
                                       (update-in path [2] CS/intersection relevant-items))]
                               (let [result-clauses (filter-index idx query)
                                     relevant-items (items-that-answer-all-conditions (map last result-clauses)
                                                                                       (count query))
                                     cleaned (map (partial mask-path-leaf-with-items relevant-items)
                                                  result-clauses)]
                                 (filter #(not-empty (last %)) cleaned))))
                     bind-variables-to-query
                     (fn [q-res index]
                       (let [seq-res-path (mapcat
                                           (fn combine-path-and-meta [from-eav-fn path]
                                             (let [expanded-path [(repeat (first path))
                                                                  (repeat (second path))
                                                                  (last path)]
                                                   meta-of-path (apply from-eav-fn (map repeat (:db/variable (meta path))))
                                                   combined (interleave meta-of-path expanded-path)]
                                               (apply (partial map vector) combined)))
                                           q-res)
                             res-path (map #(->> % (partition 2) (apply (to-eav index))) seq-res-path)]
                         (reduce #(assoc-in %1 (butlast %2) (last %2)) {} res-path)))]
                 (bind-variables-to-query q-res (indx-at db indx))))
            ind)))

(defn unify [binded-res-col needed-vars]
  (letfn [(resultify-bind-pair [vars-set accum [var-name _]]
            (if (contains? vars-set var-name) (conj accum [var-name _]) accum))
          (resultify-av-pair [vars-set accum-res av-pair]
            (reduce (partial resultify-bind-pair vars-set) accum-res av-pair))
          (locate-vars-in-query-res [vars-set [e-pair av-map]]
            (let [e-res (resultify-bind-pair vars-set [] e-pair)]
              (map (partial resultify-av-pair vars-set e-res) av-map)))]
    (map (partial locate-vars-in-query-res needed-vars) binded-res-col)))

(defmacro q
  [db query]
  `(let [pred-clauses#       (q-clauses-to-pred-clauses ~(:where query))
         needed-vars#        (symbol-col-to-set ~(:find query))
         query-plan#         (build-query-plan pred-clauses#)
         query-internal-res# (query-plan# ~db)]
     (unify query-internal-res# needed-vars#)))
