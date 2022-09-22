# cqf
A General-Purpose Counting Filter: Counting Quotient Filter (CQF)

This work appeared at SIGMOD 2017. If you use this software please cite us:
```
@inproceedings{DBLP:conf/sigmod/PandeyBJP17,
  author    = {Prashant Pandey and
               Michael A. Bender and
               Rob Johnson and
               Robert Patro},
  title     = {A General-Purpose Counting Filter: Making Every Bit Count},
  booktitle = {Proceedings of the 2017 {ACM} International Conference on Management
               of Data, {SIGMOD} Conference 2017, Chicago, IL, USA, May 14-19, 2017},
  pages     = {775--787},
  year      = {2017},
  crossref  = {DBLP:conf/sigmod/2017},
  url       = {http://doi.acm.org/10.1145/3035918.3035963},
  doi       = {10.1145/3035918.3035963},
  timestamp = {Wed, 10 May 2017 22:12:12 +0200},
  biburl    = {http://dblp.org/rec/bib/conf/sigmod/PandeyBJP17},
  bibsource = {dblp computer science bibliography, http://dblp.org}
}
```

Overview
--------
 The CQF supports approximate membership testing and counting the occurrences of
 items in a data set. This general-purpose AMQ is small and fast, has good
 locality of reference, scales out of RAM to SSD, and supports deletions,
 counting (even on skewed data sets), resizing, merging, and highly concurrent
 access.

API
--------
* `qf_insert(item, count)`: insert an item to the filter
* `qf_count_key_value(item)`: return the count of the item. Note that this
  method may return false positive results like Bloom filters or an over count.
* `qf_remove(item, count)`: decrement the count of the item by count. If count
  is 0 then completely remove the item.

Build
-------
This library depends on libssl. 

The code uses two new instructions to implement select on machine words introduced 
in intel's Haswell line of CPUs. However, there is also an alternate implementation
of select on machine words to work on CPUs older than Haswell.

To build on a Haswell or newer hardware:
```bash
 $ make test
 $ ./test 24 8
```

To build on an older hardware (older than Haswell):
```bash
 $ make NH=1 test
 $ ./test 24 8
 ```

 The argument to main is the log of the number of slots in the CQF. For example,
 to create a CQF with 2^30 slots, the argument will be 30.

Contributing
------------
Contributions via GitHub pull requests are welcome.


Authors
-------
- Prashant Pandey <ppandey@cs.stonybrook.edu>
- Rob Johnson <rob@cs.stonybrook.edu>
