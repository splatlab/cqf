#include "gqf.h"
#include "qrb.h"

#if QF_BITS_PER_SLOT == 8 || QF_BITS_PER_SLOT == 16 ||                         \
    QF_BITS_PER_SLOT == 32 || QF_BITS_PER_SLOT == 64
static inline uint64_t get_slot_remainder(const QF *qf, uint64_t index) {
  assert(index < qf->metadata->xnslots);
  return get_block(qf, index / QF_SLOTS_PER_BLOCK)
      ->slots[index % QF_SLOTS_PER_BLOCK] & NBITMASK(qf->value_bits);
}

#elif QF_BITS_PER_SLOT > 0
static inline uint64_t get_slot_remainder(const QF *qf, uint64_t index) {
  assert(index < qf->metadata->xnslots);
  return get_block(qf, index / QF_SLOTS_PER_BLOCK)
      ->slots[index % QF_SLOTS_PER_BLOCK] & NBITMASK(qf->value_bits);
}
#endif

static inline int rb_insert1(QF *qf, __uint128_t hash, uint8_t runtime_lock) {
  int ret_distance = 0;
  // TODO: Remove value from hash remainder.
  uint64_t hash_remainder_with_value = hash & BITMASK(qf->metadata->bits_per_slot); 
  uint64_t hash_remainder = hash & BITMASK(qf->metadata->bits_per_slot) & NBITMASK(qf->metadata->value_bits);
  uint64_t hash_bucket_index = hash >> qf->metadata->bits_per_slot;
  uint64_t hash_bucket_block_offset = hash_bucket_index % QF_SLOTS_PER_BLOCK;
  if (GET_NO_LOCK(runtime_lock) != QF_NO_LOCK) {
    if (!qf_lock(qf, hash_bucket_index, /*small*/ true, runtime_lock))
      return QF_COULDNT_LOCK;
  }

  if (is_empty(qf, hash_bucket_index) /* might_be_empty(qf, hash_bucket_index) && runend_index == hash_bucket_index */) {
    METADATA_WORD(qf, runends, hash_bucket_index) |=
        1ULL << (hash_bucket_block_offset % 64);
    set_slot(qf, hash_bucket_index, hash_remainder);
    METADATA_WORD(qf, occupieds, hash_bucket_index) |=
        1ULL << (hash_bucket_block_offset % 64);

    ret_distance = 0;
    modify_metadata(&qf->runtimedata->pc_noccupied_slots, 1);
    modify_metadata(&qf->runtimedata->pc_nelts, 1);
  } else {
    uint64_t runend_index = run_end(qf, hash_bucket_index);
    int operation = 0; /* Insert into empty bucket */
    uint64_t insert_index = runend_index + 1;
    uint64_t new_value = hash_remainder_with_value;
    uint64_t runstart_index =
        hash_bucket_index == 0 ? 0 : run_end(qf, hash_bucket_index - 1) + 1;
    if (is_occupied(qf, hash_bucket_index)) {
      uint64_t current_remainder = get_slot_remainder(qf, runstart_index);
      /* Skip over counters for other remainders. */
      while (current_remainder < hash_remainder &&
             runstart_index <= runend_index) {
          runstart_index++;
      }
      /* This may read past the end of the run, but the while loop
            condition will prevent us from using the invalid result in
            that case. */
      current_remainder = get_slot_remainder(qf, runstart_index);

      /* If this is the first time we've inserted the new remainder,
               and it is larger than any remainder in the run. */
      if (runstart_index > runend_index) {
        operation = 1; /* Insert at end of run */
        insert_index = runstart_index;
        new_value = hash_remainder_with_value;
        modify_metadata(&qf->runtimedata->pc_ndistinct_elts, 1);

      /* This is the first time we're inserting this remainder, but
              there are larger remainders already in the run. */
      } else if (current_remainder != hash_remainder) {
        operation = 2; /* Inserting in middle of run*/
        insert_index = runstart_index;
        new_value = hash_remainder_with_value;
        modify_metadata(&qf->runtimedata->pc_ndistinct_elts, 1);
      /* This remainder exists, we need to update it's value */
      } else {
        operation = 3; /* Update value in this slot */
        insert_index = runstart_index;
        new_value = hash_remainder_with_value;
      }
    } else {
      operation = 0; /* Insert into empty bucket */
      insert_index = runend_index + 1;
      new_value = hash_remainder_with_value;
    }
}

int rb_insert(QF *qf, uint64_t key, uint64_t value, uint8_t flags) {
  // We fill up the CQF up to 95% load factor.
  // This is a very conservative check.
  if (qf_get_num_occupied_slots(qf) >= qf->metadata->nslots * RB_MAX_LOAD_FACTOR) {
      return QF_NO_SPACE;
  }
  if (GET_KEY_HASH(flags) != QF_KEY_IS_HASH) {
    if (qf->metadata->hash_mode == QF_HASH_DEFAULT)
      key = MurmurHash64A(((void *)&key), sizeof(key), qf->metadata->seed) %
            qf->metadata->range;
    else if (qf->metadata->hash_mode == QF_HASH_INVERTIBLE)
      key = hash_64(key, BITMASK(qf->metadata->key_bits));
  }
  uint64_t hash = (key << qf->metadata->value_bits) |
                  (value & BITMASK(qf->metadata->value_bits));
  return rb_insert1(qf, hash, flags);
}
