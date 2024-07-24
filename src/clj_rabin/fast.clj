(ns clj-rabin.fast
  (:require [clojure.java.io :as io]
            [clojure.math :as math])
  (:import [java.io BufferedInputStream ByteArrayInputStream ByteArrayOutputStream File InputStream]
           [java.security MessageDigest]))

(def masks
  [0                  ;; padding
   0                  ;; padding
   0                  ;; padding
   0                  ;; padding
   0                  ;; padding
   0x0000000001804110 ;; unused except for NC 3
   0x0000000001803110 ;; 64B
   0x0000000018035100 ;; 128B
   0x0000001800035300 ;; 256B
   0x0000019000353000 ;; 512B
   0x0000590003530000 ;; 1KB
   0x0000d90003530000 ;; 2KB
   0x0000d90103530000 ;; 4KB
   0x0000d90303530000 ;; 8KB
   0x0000d90313530000 ;; 16KB
   0x0000d90f03530000 ;; 32KB
   0x0000d90303537000 ;; 64KB
   0x0000d90703537000 ;; 128KB
   0x0000d90707537000 ;; 256KB
   0x0000d91707537000 ;; 512KB
   0x0000d91747537000 ;; 1MB
   0x0000d91767537000 ;; 2MB
   0x0000d93767537000 ;; 4MB
   0x0000d93777537000 ;; 8MB
   0x0000d93777577000 ;; 16MB
   0x0000db3777577000 ;; unused except for NC 3
   ])

(defn random-long
  ^long [^long seed]
  (let [bytes (byte-array (repeat 64 (inc seed)))
        algorithm (doto (MessageDigest/getInstance "MD5")
                    (.reset)
                    (.update bytes))]
    (.longValue (BigInteger. 1 (.digest algorithm)))))

(defn shift-once
  ^long [^long l]
  (bit-shift-left l 1))

(defn shift-twice
  ^long [^long l]
  (bit-shift-left l 2))

(def lookups
  (let [gear (mapv random-long (range 255))]
    {:gear gear
     :shift (mapv shift-once gear)}))

(defprotocol RabinHashable
  (-hash-seq [this ctx] "returns a lazy-seq of chunks for this"))

(defn drain
  [n src dest]
  (let [bytes (if (= 1 n) (.read src) (.readNBytes src n))]
    (when-not (= -1 bytes)
      (.write dest bytes)
      bytes)))

(defn maskset
  [center level]
  (let [bits (int (quot (math/log10 center) (math/log10 2)))
        ;;_ (prn :avg center :bits bits :n level)
        small-mask (masks (+ bits ^long level))
        large-mask (masks (- bits ^long level))]
    {:small
     {:gear small-mask
      :shift (shift-once small-mask)}

     :large
     {:gear large-mask
      :shift (shift-once large-mask)}}))

(defn- chunker
  [^BufferedInputStream this {:size/keys [^long min ^long average ^long max]
                              :keys [pos ^long normalization]
                              :or {pos 0
                                   normalization 0
                                   min 64
                                   average 256
                                   max 1024}}]
  (let [whats-left (.available this)
        masks (maskset average normalization)]
    (if (<= whats-left min)
      {:data (.readNBytes this whats-left)
       :fingerprint 0
       :offset pos
       :length whats-left}
      (with-open [baos (ByteArrayOutputStream.)]
        (let [^long upper-bound (if (> whats-left max) max whats-left)
              ^long normal (if (< whats-left average) whats-left average)
              data-fn (fn [fingerprint length]
                        {:data (.toByteArray baos)
                         :length length
                         :fingerprint fingerprint
                         :offset pos})]
          #_(prn :remainder whats-left
                 :min min :avg average :max max
                 :upper-bound upper-bound
                 :normal normal
                 :start (/ min 2)
                 :end (/ upper-bound 2))
          (drain min this baos)
          (loop [fingerprint 0
                 length min
                 [type & rest] (cycle [:shift :gear])]
            (if (= length upper-bound)
              (data-fn fingerprint length)
              (let [mask (if (< length ^long normal)
                           (masks :small)
                           (masks :large))
                    byte (drain 1 this baos)
                    length (inc length)
                    fingerprint (cond-> fingerprint
                                  (#{:shift} type) (shift-twice)
                                  true (unchecked-add ^long (get-in lookups [type byte])))]
                (if (zero? (bit-and fingerprint ^long (mask type)))
                  (data-fn fingerprint length)
                  (recur fingerprint length rest)))))

          #_(reduce
             (fn [{:keys [fingerprint length] :as acc} i]
               (prn :i i :l length :b upper-bound :r (.available this))
               (if (= length upper-bound)
                 (reduced (assoc acc :data (.toByteArray baos) :length length))
                 (let [mask (if (< length ^long normal)
                              (masks :small)
                              (masks :large))
                       byte (drain 1 this baos)
                       length (inc length)
                       fingerprint (unchecked-add (shift-twice fingerprint) ^long (get-in lookups [:shift byte]))
                     ;;_ (prn :shift-stage length byte fingerprint (mask :shift) (zero? (bit-and fingerprint ^long (mask :shift))))
                       ]
                   (if (or (zero? (bit-and fingerprint ^long (mask :shift)))
                           (zero? (.available this)))
                     (reduced (assoc acc :data (.toByteArray baos) :fingerprint fingerprint :length length))
                     (let [byte (drain 1 this baos)
                           length (inc length)
                           ;; _ (prn fingerprint byte)
                           fingerprint (unchecked-add fingerprint ^long (get-in lookups [:gear byte]))]
                       (if (zero? (bit-and fingerprint (mask :normal)))
                         (reduced (assoc acc :data (.toByteArray baos) :fingerprint fingerprint :length length))
                         (assoc acc :fingerprint fingerprint :length length)))))))
             {:fingerprint 0 :offset pos :length min}
             (range))

          #_(reduce (fn [fingerprint ^long i]
                      (let [^long mask (if (< i ^long normal-cutoff)
                                         (masks :small)
                                         (masks :large))
                            offset (* 2 i)
                            byte (drain 1 this baos)
                            ;; _ (prn :shift-stage offset byte fingerprint)
                            fingerprint (unchecked-add (shift-twice fingerprint) ^long (get-in lookups [:shift byte]))]
                        (if (or (zero? (bit-and fingerprint mask))
                                ;; if we've hit the end, return regardless
                                (>= (dec upper-cutoff) offset))
                          (reduced
                           {:data (.toByteArray baos)
                            :fingerprint fingerprint
                            :offset pos
                            :length (inc offset)})
                          (let [byte (drain 1 this baos)
                                ;;_ (prn fingerprint byte)
                                fingerprint (unchecked-add fingerprint ^long (get-in lookups [:gear byte]))]
                            (if (zero? (bit-and fingerprint mask))
                              (reduced
                               {:data (.toByteArray baos)
                                :fingerprint fingerprint
                                :offset pos
                                :length (+ 2 offset)})
                              fingerprint)))))
                    0
                    indices))))))

(extend BufferedInputStream
  RabinHashable
  {:-hash-seq
   (fn [^BufferedInputStream this {:keys [chunk-fn] :or {chunk-fn chunker} :as ctx}]
     (lazy-seq
      (when (pos? (.available this))
        (when-let [{:keys [length] :as chunk} (chunk-fn this ctx)]
          #_(prn chunk ctx)
          (cons chunk (-hash-seq this (update ctx :pos (fnil + 0) length)))))))})

(extend InputStream
  RabinHashable
  {:-hash-seq
   (fn [^InputStream this ctx]
     (-hash-seq (BufferedInputStream. this) ctx))})

(extend (class (make-array Byte/TYPE 0))
  RabinHashable
  {:-hash-seq
   (fn [^bytes this ctx]
     (-hash-seq (ByteArrayInputStream. this) ctx))})

(extend File
  RabinHashable
  {:-hash-seq
   (fn [^File this ctx]
     (when (and (.exists this) (.isFile this))
       #_(prn :f this (.length this))
       (try
         (doall (-hash-seq (io/input-stream this) ctx))
         (catch Exception e
           (prn this e)))))})

(extend String
  RabinHashable
  {:-hash-seq
   (fn [^String this ctx]
     (-hash-seq (.getBytes this) ctx))})

(comment
  "data/enron/maildir/arnold-j/deleted_items/397."
  "data/enron/maildir/arnold-j/inbox/34."
  "data/enron/maildir/arnold-j/discussion_threads/81."
  "data/enron/maildir/arnold-j/vulcan_signs/2."

  "data/enron/maildir/arnold-j/notes_inbox/9."

  (def f (io/file "/Users/ddouglass/src/clj-rabin/data/enron/maildir/arnold-j/vulcan_signs/2."))
  (-hash-seq f {})

  nil)
