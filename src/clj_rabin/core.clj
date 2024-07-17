(ns clj-rabin.core
  (:require [clojure.math :as math]))

(def default-rabin-ctx
  {; should be close to the number of possible values, 257 is closest to 255
   :prime 257
   ; modulus, should also be prime, suggested via moinakg via SREP tool
   :q     153191})

(defn window-pow
  "pow = p^window-sz % q"
  [{:keys [window-size prime q]}]
  (-> (math/pow prime window-size)
      (mod q)))

(defn poly-hash
  "Compute the polynomial hash for a window
  P^w*a[i] + P^w-1[i+1] + ..."
  [{:keys [window-size prime q]} ^bytes bs]
  (let [hash (reduce (fn [acc i]
                       (-> (math/pow prime (- (dec window-size) i))
                           (* (nth bs i))
                           (+ acc)))
                     0
                     (range window-size))]
    (mod hash q)))

(defn rolling-hash-seq
  "Given a rabin context and some bytes, emit a seq of rabin hashes at
  each index beginning from the end of the first sliding window"
  [{:keys [window-size prime q] :as ctx} ^bytes bs]
  (let [window-size (if (>= window-size (alength bs))
                      (dec (alength bs))
                      window-size)
        pow (window-pow ctx)
                        ; NOTE: reductions emits the initial value, so we do not have to cons
        ; the first window's hash and index to the list
        hashes (reductions
                 (fn [acc i]
                   (let [out-byte (nth bs (- i window-size))
                         in-byte (nth bs i)]
                     (-> (* acc prime)
                         (+ in-byte)
                         (- (* out-byte pow))
                         (mod q))))
                 (poly-hash ctx bs)
                 (range window-size (alength bs)))]
    ; ...and that is why we dec here
    (->> (interleave (range (dec window-size) (alength bs)) hashes)
         (partition-all 2))))

(comment
  ; find common windows in O(n)
  (let [some-data (.getBytes "abcdefghabcdefzabcdz5")
        {:keys [window-size] :as rabin-ctx} (assoc default-rabin-ctx :window-size 6)
        hash-seq (rolling-hash-seq rabin-ctx some-data)
        groups (->> (group-by last hash-seq)
                    (into {} (map (fn [[k v]]
                                    [k (map (comp inc first) v)]))))]
    (map (fn [[h is]]
           [h (map #(String. (byte-array (subvec (vec some-data) (- % window-size) %))) is)])
         groups)))
