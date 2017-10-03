# cqf
A General-Purpose Counting Filter: Counting Quotient Filter (CQF)

Overview
--------
 The CQF supports approximate membership testing and counting the occurrences of
 items in a data set. This general-purpose AMQ is small and fast, has good
 locality of reference, scales out of RAM to SSD, and supports deletions,
 counting (even on skewed data sets), resizing, merging, and highly concurrent
 access.

 This is a threadsafe implementation of the CQF. Currently, we only support
 multiple update operations (i.e., insert and remove) at the same time. Query
 operation doesn't acquire a lock.  Therefore, updates and queries can not
 operate simultaneously.

API
--------
* 'qf_insert(item, count)': insert an item to the filter.
* 'qf_count_key_value(item)': return the count of the item. Note that this
  method may return false positive results like Bloom filters or an over count.
* 'qf_remove(item, count)': decrement the count of the item by count. If count
  is 0 then completely remove the item.

Build
-------
This library depends on libssl. 

The code uses two new instructions to implement select on machine words
introduced in intel's Haswell line of CPUs. However, there is also an
alternate implementation of select on machine words to work on CPUs older than
Haswell.

To build on a Haswell or newer hardware:
```bash
 $ make
 $ ./main 24 3 4
```

To build on an older hardare (older than Haswell):
```bash
 $ make NH=1
 $ ./main 24 3 4
 ```

 Following are the arguments to main:
 * Size of the CQF: the log of the number of slots in the CQF. For example,
 to create a CQF with 2^30 slots, the argument will be 30.
 * Multiplicity of elements: the multiplicity of each distinct element to be
 inserted.
 * Number of threads: number of threads performing insertions.

 The main program creates a filter and inserts random elements with the given
 multiplicity using the given number of threads. It also stores those elements
 in a std::set and std::multiset. After all insertions are done it verifies the
 total number of distinct elements and the multiplicity of each element in the
 filter using the set and multiset.

Contributing
------------
Contributions via GitHub pull requests are welcome.


Authors
-------
- Prashant Pandey <ppandey@cs.stonybrook.edu>
- Rob Johnson <rob@cs.stonybrook.edu>
