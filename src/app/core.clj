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
 
