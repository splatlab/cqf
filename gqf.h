/*
 * =====================================================================================
 *
 *       Filename:  gqf.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2017-02-04 03:40:58 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Prashant Pandey (ppandey@cs.stonybrook.edu)
 *                  Rob Johnson (rob@cs.stonybrook.edu)
 *   Organization:  Stony Brook University
 *
 * =====================================================================================
 */

#ifndef QF_H
#define QF_H

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BITS_PER_SLOT 8

    struct __attribute__ ((__packed__)) qfblock;
    typedef struct qfblock qfblock;

    uint64_t shift_into_b2(uint64_t a, uint64_t b, int bstart, int bend, int amount);

#ifdef LOG_NUM_SHIFTS
#define len 5000
int shift_count[len];
#endif

    typedef struct quotient_filter {
        uint64_t nslots;
        uint64_t xnslots;
        uint64_t key_bits;
        uint64_t value_bits;
        uint64_t key_remainder_bits;
        uint64_t bits_per_slot;
        __uint128_t range;
        uint64_t nblocks;
        uint64_t nelts;
        uint64_t ndistinct_elts;
        uint64_t noccupied_slots;
        qfblock *blocks;
    } quotient_filter;


    typedef quotient_filter QF;

    typedef struct quotient_filter_iterator {
        const QF *qf;
        uint64_t run;
        uint64_t current;
    } quotient_filter_iterator;

    typedef quotient_filter_iterator QFi;

    void qf_init(QF *qf, uint64_t nslots, uint64_t key_bits, uint64_t value_bits);

    void qf_destroy(QF *qf);

    /* Increment the counter for this key/value pair by count. */
    void qf_insert(QF *qf, uint64_t key, uint64_t value, uint64_t count);

    /* Remove count instances of this key/value combination. */
    void qf_remove(QF *qf, uint64_t key, uint64_t value, uint64_t count);

    /* Remove all instances of this key/value pair. */
    void qf_delete_key_value(QF *qf, uint64_t key, uint64_t value);

    /* Remove all instances of this key. */
    void qf_delete_key(QF *qf, uint64_t key);

    /* Replace the association (key, oldvalue, count) with the association
         (key, newvalue, count). If there is already an association (key,
         newvalue, count'), then the two associations will be merged and
         their counters will be summed, resulting in association (key,
         newvalue, count' + count). */
    void qf_replace(QF *qf, uint64_t key, uint64_t oldvalue, uint64_t newvalue);

    /* Lookup the value associated with key.  Returns the count of that
         key/value pair in the QF.  If it returns 0, then, the key is not
         present in the QF. Only returns the first value associated with key
         in the QF.  If you want to see others, use an iterator. */
    uint64_t qf_query(const QF *qf, uint64_t key, uint64_t *value);

    /* Return the number of times key has been inserted, with any value,
         into qf. */
    uint64_t qf_count_key(const QF *qf, uint64_t key);

    /* Return the number of times key has been inserted, with the given
         value, into qf. */
    uint64_t qf_count_key_value(const QF *qf, uint64_t key, uint64_t value);

    /* Initialize an iterator */
    void qf_iterator(const QF *qf, QFi *qfi, uint64_t position);

    /* Returns 0 if the iterator is still valid (i.e. has not reached the
         end of the QF. */
    int qfi_get(const QFi *qfi, uint64_t *key, uint64_t *value, uint64_t *count);

    /* Advance to next entry.  Returns whether or not another entry is
         found.  */
    int qfi_next(QFi *qfi);

    /* Check to see if the if the end of the QF */
    int qfi_end(const QFi *qfi);

    /* For debugging */
    void qf_dump(const QF *);

    /* write data structure of to the disk */
    void qf_serialize(const QF *qf, const char *filename);

    /* read data structure off the disk */
    void qf_deserialize(QF *qf, const char *filename);

    /* merge two QFs into the third one. */
    void qf_merge(const QF *qfa, const QF *qfb, QF *qfc);

    /* merge multiple QFs into the final QF one. */
    void qf_multi_merge(QF *qf_arr[], int nqf, QF *qfr);

#ifdef __cplusplus
} // extern 'C'
#include <array>
#include <cstring>
namespace qf {
class filter {
    QF filt_;
    class iterator {
        QFi it_;
        using TupleType = std::array<uint64_t, 3>;
    public:
        iterator(const filter *filter, uint64_t pos) {
            qf_iterator((const QF *)filter, &static_cast<QFi &>(*this), pos);
        }
        iterator &operator++() {qfi_next(&(QFi &)(*this)); return *this;}
        iterator operator++(int) {
            iterator ret(*this);
            operator++();
            return ret;
        }
        operator QFi &()       {return *(QFi *)this;}
        operator const QFi &() const {return *(const QFi *)this;}
        iterator(const iterator &it) {
            std::memcpy(this, &it, sizeof(it));
        }
        int get(uint64_t *key, uint64_t *value, uint64_t *count) const {
#ifdef __EXCEPTIONS
            if(qfi_get(&static_cast<const QFi &>(*this), key, value, count)) throw std::runtime_error("Iterator is exhausted.");
            return 0;
#else
            return qfi_get(&static_cast<QFi &>(*this), key, value, count);
#endif
        }
        TupleType get() const {
            std::array<uint64_t, 3> ret;
            get(ret);
            return ret;
        }
        TupleType operator*() const {return get();}
        void get(TupleType &tup) const { // Modify tuple in-place.
            get(&tup[0], &tup[1], &tup[2]);
        }
        template<typename T>
        bool operator==([[maybe_unused]] const T &val) const {
            return end();
        }
        template<typename T>
        bool operator!=([[maybe_unused]] const T &val) const {
            return !operator==(val);
        }
        int end() const {
            return qfi_end(&static_cast<const QFi &>(*this));
        }
    };
public:
    operator QF &() {
        return *(QF *)this;
    }
    operator const QF &() {
        return *(const QF *)this;
    }
    filter(uint64_t nslots, uint64_t key_bits, uint64_t value_bits) {
        qf_init(&filt_, nslots, key_bits, value_bits);
    }
    filter(const char *filename) {
        qf_deserialize(&filt_, filename);
    }
    iterator begin() {
        return iterator(this, 0);
    }
    iterator end() const {
        return iterator(this, 0);
    }
    void insert(uint64_t key, uint64_t value, uint64_t count) {
        qf_insert(&filt_, key % filt_.range, value, count);
    }
    void remove(uint64_t key, uint64_t value, uint64_t count) {
        qf_remove(&filt_, key % filt_.range, value, count);
    }
    uint64_t count(uint64_t key) const {
        return qf_count_key(&filt_, key % filt_.range);
    }
    uint64_t count(uint64_t key, uint64_t value) const {
        return qf_count_key_value(&filt_, key, value);
    }
    void dump() const {
        qf_dump(&filt_);
    }
    void query(uint64_t key, uint64_t *value) const {
        qf_query(&filt_, key, value);
    }
    uint64_t query(uint64_t key) const {
        uint64_t ret;
        query(key, &ret);
        return ret;
    }
    void replace(uint64_t key, uint64_t oldvalue, uint64_t newvalue) {
        qf_replace(&filt_, key, oldvalue, newvalue);
    }
    void del(uint64_t key) {
        qf_delete_key(&filt_, key);
    }
    void del(uint64_t key, uint64_t value) {
        qf_delete_key_value(&filt_, key, value);
    }
    ~filter() {
        qf_destroy(&filt_);
    }
};
} // namespace qf
#endif

#endif /* QF_H */
