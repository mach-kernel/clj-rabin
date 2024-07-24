3M(ns clj-rabin.fast
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
  (-hash-seq "returns a lazy-seq of chunks for this" [this ctx]))

(defn drain
  [n src dest]
  (let [bytes (.readNBytes src n)]
    (.write dest bytes)
    bytes))

(defn- chunker
  [^BufferedInputStream this {:size/keys [expected min max]
                              :keys [pos normalization]}]
  (let [whats-left (.avaliable this)
        bits (quot (math/log10 expected) (math/log10 2))
        masks {:small (masks (+ bits normalization))
               :large (masks (- bits normalization))}]
    (if (<= whats-left min)
      {:data (.readNBytes this whats-left)
       :fingerprint 0
       :offset pos
       :length whats-left}
      (with-open [baos (ByteArrayOutputStream.)]
        (let [upper-bound (if (> whats-left max) max whats-left)
              normal (if (< whats-left expected) whats-left expected)
              normal-cutoff (quot normal 2)]
          (drain min this baos)
          (reduce (fn [fingerprint i]
                    (let [mask (if (< i normal-cutoff)
                                 (masks :small)
                                 (masks :large))
                          offset (* 2 i)
                          byte (drain 1 this baos)
                          fingerprint (+ (shift-twice fingerprint) (get-in lookups [:shift byte]))]
                      (if (zero? (bit-and fingerprint mask))
                        (reduced
                         {:data (.toByteArray baos)
                          :fingerprint fingerprint
                          :offset pos
                          :length offset})
                        (let [offset (inc offset)
                              byte (drain 1 this baos)
                              fingerprint (+ fingerprint (get-in lookups [:gear byte]))]
                          (if (map? chunk)
                            (reduced
                             {:data (.toByteArray baos)
                              :fingerprint fingerprint
                              :offset pos
                              :length offset})
                            chunk)))))
                  0
                  (range (quot min 2) (quot upper-bound 2)))))))

  (extend BufferedInputStream
    RabinHashable
    {:-hash-seq
     (fn [^BufferedInputStream this {:keys [chunk-fn] :or {chunk-fn chunker} :as ctx}]
       (lazy-seq
        (when (pos? (.available this))
          (when-let [{:keys [length] :as chunk} (chunk-fn this ctx)]
            (cons chunk (-hash-seq this (update ctx :pos + length)))))))}))

(extend InputStream
  RabinHashable
  {:-hash-seq
   (fn [^InputStream this ctx]
     (-hash-seq (BufferedInputStream. this) ctx))})

(extend bytes
  RabinHashable
  {:-hash-seq
   (fn [^bytes this ctx]
     (-hash-seq (ByteArrayInputStream. this) ctx))})

(extend File
  RabinHashable
  {:-hash-seq
   (fn [^File this ctx]
     (when (and (.exists this) (.isFile this))
       (-hash-seq (io/input-stream this) ctx)))})

(extend String
  RabinHashable
  {:-hash-seq
   (fn [^String this ctx]
     (-hash-seq (.getBytes this) ctx))})