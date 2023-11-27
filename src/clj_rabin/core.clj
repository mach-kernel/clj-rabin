(ns clj-rabin.core
  (:require [clojure.math :as math]))

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
  [{:keys [window-size prime q] :as ctx} ^bytes bs]
  (let [window-size (if (>= window-size (alength bs))
                      (dec (alength bs))
                      window-size)
        pow (window-pow ctx)
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
    (->> (interleave (range (dec window-size) (alength bs)) hashes)
         (partition-all 2))))

(comment
  ; find common windows in O(n)
  (let [some-data (.getBytes "abcdefghabcdefzabcdz5")
        {:keys [window-size] :as rabin-ctx} {:window-size 3 :prime 153191 :q 139907}
        hash-seq (rolling-hash-seq rabin-ctx some-data)
        groups (->> (group-by last hash-seq)
                    (into {} (map (fn [[k v]]
                                    [k (map (comp inc first) v)]))))]
    (map (fn [[h is]]
           [h (map #(String. (byte-array (subvec (vec some-data) (- % window-size) %))) is)])
         groups)))
