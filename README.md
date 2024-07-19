# clj-rabin

A Clojure implementation of a rolling Rabin hash + chunker.

#### Hacking

Bring up a REPL:
```
lein with-profile +dev 
```

Use the CDC notebook to try chunking a dataset:

```clojure
(comment
  (def chunks
    (atom nil))

  (load-dataset! "data/natural_images" chunks)

  (let [rows->long (fn [r] (into {} (map (fn [[k v]] [k (long v)])) r))
        stats-all (chunk-ds->agg-stats @chunks)
        stats-cdc (-> @chunks (ds/unique-by-column :sha256) chunk-ds->agg-stats)
        stats-per-file (->> @chunks
                            (rd/group-by-column-agg :file {:block-count (rd/count-distinct :sha256)})
                            (rd/aggregate {:avg-blocks-per-file (rd/mean :block-count)}))

        ; maps containing aggregate vals
        all (first (map rows->long (ds/rows stats-all)))
        cdc (first (map rows->long (ds/rows stats-cdc)))
        blocks (first (map rows->long (ds/rows stats-per-file)))
        reduced-bytes (- (:total-bytes all)
                         (:total-bytes cdc))]
    {:all all
     :cdc cdc
     :blocks blocks
     :diff {:reduced-bytes reduced-bytes
            :reduced-percent (->> (:total-bytes all)
                                  (/ reduced-bytes)
                                  (* 100)
                                  double)}}))
```

`@chunks` is a `tech.ml.dataset` with all the chunks from the data loaded. Each chunk also gets a SHA-256 hash to ensure that the block is actually unique:

```clojure
{:all {:total-bytes 359403192, :total-blocks 36628, :avg-block-size-bytes 9812},
 :cdc {:total-bytes 179670613, :total-blocks 18311, :avg-block-size-bytes 9812},
 :blocks {:avg-blocks-per-file 2},
 :diff {:reduced-bytes 179732579, :reduced-percent 50.00862068025261}}
```

##### Datasets tested

Rabin parameter overrides are shown, otherwise assume defaults from `clj-rabin.hash/default-ctx`.


###### Audio

Overall: terrible ratios

[Million song dataset](https://www.kaggle.com/datasets/undefinenull/million-song-dataset-spotify-lastfm)

Audio codec info:

```
Input #0, mp3, from 'MP3-Example/Blues/Blues-TRADWSG128F4259317.mp3':
  Duration: 00:00:30.04, start: 0.025057, bitrate: 96 kb/s
  Stream #0:0: Audio: mp3, 44100 Hz, stereo, fltp, 96 kb/s
    Metadata:
      encoder         : LAME3.99r
    Side data:
      replaygain: track gain - -5.900000, track peak - unknown, album gain - unknown, album peak - unknown, 
```

```clojure
{:all {:total-bytes 544824272, :total-blocks 977662, :avg-block-size-bytes 557},
 :cdc {:total-bytes 543861899, :total-blocks 43428, :avg-block-size-bytes 12523},
 :blocks {:avg-blocks-per-file 651},
 :diff {:reduced-bytes 962373, :reduced-percent 0.1766391567811795}}
```

[Indian Music Raga](https://www.kaggle.com/datasets/kcwaghmarewaghmare/indian-music-raga)

Audio codec info:
```
Input #0, wav, from 'raga/asavari02.wav':
  Duration: 00:03:46.82, bitrate: 705 kb/s
  Stream #0:0: Audio: pcm_s16le ([1][0][0][0] / 0x0001), 44100 Hz, 1 channels, s16, 705 kb/s
```

```clojure
{:all {:total-bytes 1105687312, :total-blocks 73031, :avg-block-size-bytes 15139},
 :cdc {:total-bytes 1105687295, :total-blocks 73014, :avg-block-size-bytes 15143},
 :blocks {:avg-blocks-per-file 890},
 :diff {:reduced-bytes 17, :reduced-percent 1.537505207439696E-6}}
```

###### Images

Overall: awesome ratios on images

[Natural images](https://www.kaggle.com/datasets/prasunroy/natural-images)

```clojure
{:all {:total-bytes 359403192, :total-blocks 36628, :avg-block-size-bytes 9812},
 :cdc {:total-bytes 179670613, :total-blocks 18311, :avg-block-size-bytes 9812},
 :blocks {:avg-blocks-per-file 2},
 :diff {:reduced-bytes 179732579, :reduced-percent 50.00862068025261}}
```

[Ripe and unripe tomatoes (images)](https://www.kaggle.com/datasets/sumn2u/riped-and-unriped-tomato-dataset)

```clojure
{:all {:total-bytes 122864035, :total-blocks 151613, :avg-block-size-bytes 810},
 :cdc {:total-bytes 52075246, :total-blocks 29899, :avg-block-size-bytes 1741},
 :blocks {:avg-blocks-per-file 428},
 :diff {:reduced-bytes 70788789, :reduced-percent 57.6155495788495}}
```

#### Resources / Credits

- [moinakg pcompress article](https://moinakg.wordpress.com/tag/rabin-fingerprint/).
- [ncona](https://ncona.com/2017/06/the-rabin-karp-algorithm/)
- [YADL](https://github.com/YADL/yadl/wiki/Rabin-Karp-for-Variable-Chunking)
- [Horner's method](https://en.wikipedia.org/wiki/Horner%27s_method)
- [how does rabin-karp choose breakpoint in variable-length chunking? (SO)](https://stackoverflow.com/questions/67101553/how-does-rabin-karp-choose-breakpoint-in-variable-length-chunking)
  - [TTTD Paper](https://www.hpl.hp.com/techreports/2005/HPL-2005-30R1.pdf)
- [Why is Rabin base a prime?](https://cs.stackexchange.com/a/28024)
- [Choosing modulus in Rabin-Karp](https://cs.stackexchange.com/questions/10174/how-do-we-find-the-optimal-modulus-q-in-rabin-karp-algorithm)
