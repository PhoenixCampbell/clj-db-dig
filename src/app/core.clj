(defrecord Database [layers top-id curr-time])
(defrecord Layer [Storage VAET AVET VEAT EAVT])
(defrecord Entity [id attrs])

(defn make-entity
  ([] (make-entity :db/no-id-yet))
  ([id] (Entity. id {})))

(defrecord Attr [name value ts prev-ts])

(defn make-attr
  ([name value type ; these ones are required
    & {:keys [cardinality] :or {cardinality :db/single}} ]
   {:pre [(contains? #{:db/single :db/multiple} cardinality)]}
   (with-meta (Attr. name value -1 -1) {:type type :cardinality cardinality})))

(defn add-attr [ent attr]
  (let [attr-id (keyword (:name attr))]
    (assoc-in ent [:attrs attr-id] attr)))

(defprotocol Storage
  (get-entity [storage e-id] )
  (write-entity [storage entity])
  (drop-entity [storage entity]))

(defrecord InMemory [] Storage
           (get-entity [storage e-id] (e-id storage))
           (write-entity [storage entity] (assoc storage (:id entity) entity))

           (drop-entity [storage entity] (dissoc storage (:id entity))))

(defn make-index [from-eav to-eav usage-pred]
  (with-meta {} {:from-eav from-eav :to-eav to-eav :usage-pred usage-pred}))
(defn from-eav [index] (:from-eav (meta index)))
(defn o-eav [index] (:to-eav (meta index)))
(defn usage-pred [index] (:usage-pred (meta index)))

(defn indexes[] [:VAET :AVET :VEAT :EAVT])

(defn ref? [attr] (= :db/ref (:type (meta attr))))

(defn always[& more] true)

(defn make-db []
  (atom
   (Database. [(Layer.
                (fdb.storage.InMemory.) ; storage
                (make-index #(vector %3 %2 %1) #(vector %3 %2 %1) #(ref? %));VAET
                (make-index #(vector %2 %3 %1) #(vector %3 %1 %2) always);AVET                        
                   (make-index #(vector %3 %1 %2) #(vector %2 %3 %1) always);VEAT                       
                   (make-index #(vector %1 %2 %3) #(vector %1 %2 %3) always);EAVT
                )] 0 0)))

(defn entity-at
  ([db ent-id] (entity-at db (:curr-time db) ent-id))
  ([db ts ent-id] (get-entity (get-in db [:layers ts :storage]) ent-id)))

(defn attr-at
  ([db ent-id atr-name] (attr-at db ent-id atr-name (:curr-time db)))
  ([db ent-id attr-name ts] (get-in (entity-at db ts ent-id) [:attrs attr-name])))

(defn value-of-at
  ([db ent-id attr-name] (:value (attr-at db ent-id attr-name)))
  ([db ent-id attr-name ts] (:value (attr-at db ent-id attr-name ts))))

(defn indx-at
  ([db kind] (indx-at db kind (:curr-time db)))
  ([db kind ts] (kind ((:layers db) ts))))

(defn evolution-of [db ent-id attr-name]
  (loop [res [] ts (:curr-time db)]
    (if (= -1 ts) (reverse res)
        (let [attr (attr-at db ent-id attr-name ts)]
          (recur (conj res {(:ts attr) (:value attr)}) (:prev-ts attr))))))

(defn add-entity [db ent]
   (let [[fixed-ent next-top-id] (fix-new-entity db ent)
         layer-with-updated-storage (update-in 
                            (last (:layers db)) [:storage] write-entity fixed-ent)
         add-fn (partial add-entity-to-index fixed-ent)
         new-layer (reduce add-fn layer-with-updated-storage (indexes))]
    (assoc db :layers (conj (:layers db) new-layer) :top-id next-top-id)))

(defn- next-ts [db] (inc (:curr-time db)))

(defn- update-creation-ts [ent ts-val]
   (reduce #(assoc-in %1 [:attrs %2 :ts ] ts-val) ent (keys (:attrs ent))))

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

(defn- add-entity-to-index [ent layer ind-name]
   (let [ent-id (:id ent)
         index (ind-name layer)
         all-attrs  (vals (:attrs ent))
         relevant-attrs (filter #((usage-pred index) %) all-attrs)
         add-in-index-fn (fn [ind attr] 
                                 (update-attr-in-index ind ent-id (:name attr) 
                                                                  (:value attr) 
                                                                  :db/add))]
        (assoc layer ind-name  (reduce add-in-index-fn index relevant-attrs))))

(defn- update-attr-in-index [index ent-id attr-name target-val operation]
   (let [colled-target-val (collify target-val)
         update-entry-fn (fn [ind vl] 
                             (update-entry-in-index 
                                ind 
                                ((from-eav index) ent-id attr-name vl) 
                                operation))]
     (reduce update-entry-fn index colled-target-val)))

(defn- update-entry-in-index [index path operation]
   (let [update-path (butlast path)
         update-value (last path)
         to-be-updated-set (get-in index update-path #{})]
     (assoc-in index update-path (conj to-be-updated-set update-value))))

(defn add-entities [db ents-seq] (reduce add-entity db ents-seq))

(defn remove-entity [db ent-id]
   (let [ent (entity-at db ent-id)
         layer (remove-back-refs db ent-id (last (:layers db)))
         no-ref-layer (update-in layer [:VAET] dissoc ent-id)
         no-ent-layer (assoc no-ref-layer :storage 
                                   (drop-entity  
                                          (:storage no-ref-layer) ent))
         new-layer (reduce (partial remove-entity-from-index ent) 
                                 no-ent-layer (indexes))]
     (assoc db :layers (conj  (:layers db) new-layer))))

(defn- remove-back-refs [db e-id layer]
   (let [reffing-datoms (reffing-to e-id layer)
         remove-fn (fn[d [e a]] (update-entity db e a e-id :db/remove))
         clean-db (reduce remove-fn db reffing-datoms)]
     (last (:layers clean-db))))

(defn- reffing-to [e-id layer]
   (let [vaet (:VAET layer)]
         (for [[attr-name reffing-set] (e-id vaet)
               reffing reffing-set]
              [reffing attr-name])))