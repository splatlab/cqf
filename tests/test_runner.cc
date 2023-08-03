#include <fstream>
#include <iostream>
#include <map>
#include <openssl/rand.h>
#include <set>
#include <unistd.h>
#include <vector>
#include <cassert>
#include "rhm_wrapper.h"
#include "trhm_wrapper.h"

using namespace std;

#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits) ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))


typedef int (*init_op)(uint64_t nkeys, uint64_t key_bits, uint64_t value_bits);
typedef int (*insert_op)(uint64_t key, uint64_t val);
typedef int (*lookup_op)(uint64_t key, uint64_t *val);
typedef int (*remove_op)(uint64_t key);
typedef int (*rebuild_op)();
typedef int (*destroy_op)();

typedef struct hashmap {
  init_op init;
  insert_op insert;
  lookup_op lookup;
  remove_op remove;
  rebuild_op rebuild;
  destroy_op destroy;
} hashmap;

hashmap rhm = {g_rhm_init, g_rhm_insert, g_rhm_lookup, g_rhm_remove,
               g_rhm_rebuild, g_rhm_destroy};
hashmap trhm = {g_trhm_init, g_trhm_insert, g_trhm_lookup, g_trhm_remove,
               g_trhm_rebuild, g_trhm_destroy};

#define INSERT 0
#define DELETE 1
#define LOOKUP 2

struct hm_op {
  int op;
  uint64_t key;
  uint64_t value;
};

void load_ops(std::string replay_filepath, int *key_bits, int *quotient_bits,
              int *value_bits, std::vector<hm_op> &ops) {
  ifstream ifs;
  ifs.open(replay_filepath);
  ifs >> (*key_bits) >> (*quotient_bits) >> (*value_bits);
  uint64_t num_keys;
  ifs >> num_keys;
  for (int i = 0; i < num_keys; i++) {
    hm_op op;
    ifs >> op.op >> op.key >> op.value;
    ops.push_back(op);
  }
  ifs.close();
}

void write_ops(std::string replay_filepath, int key_bits, int quotient_bits,
               int value_bits, std::vector<hm_op> &ops) {
  ofstream ofs;
  ofs.open(replay_filepath);
  ofs << key_bits << " " << quotient_bits << " " << value_bits << std::endl;
  ofs << ops.size() << std::endl;
  for (auto op : ops) {
    ofs << op.op << " " << op.key << " " << op.value << std::endl;
  }
  ofs.close();
}

uint64_t get_random_key(std::map<uint64_t, uint64_t> map, int key_bits) {
  uint64_t rand_idx = 0;
  rand_idx = rand_idx % map.size();
  // This is not the best way to do this.
  auto iter = map.begin();
  std::advance(iter, rand_idx);
  return iter->first;
}

void generate_ops(int key_bits, int quotient_bits, int value_bits,
                  int initial_load_factor, int num_ops,
                  std::vector<hm_op> &ops) {
  uint64_t nkeys = ((1ULL << quotient_bits) * initial_load_factor) / 100;
  uint64_t *keys = new uint64_t[nkeys];
  uint64_t *values = new uint64_t[nkeys];

  RAND_bytes((unsigned char *)keys, nkeys * sizeof(uint64_t));
  RAND_bytes((unsigned char *)values, nkeys * sizeof(uint64_t));

  std::map<uint64_t, uint64_t> map;
  std::vector<uint64_t> deleted_keys;

  for (int i = 0; i < nkeys; i++) {
    uint64_t key = keys[i] & BITMASK(key_bits);
    uint64_t value = values[i] & BITMASK(value_bits);
    ops.push_back({INSERT, key, value});
    map[key] = value;
  }

  delete keys;
  delete values;

  values = new uint64_t[num_ops];
  keys = new uint64_t[num_ops];
  RAND_bytes((unsigned char *)values, num_ops * sizeof(uint64_t));
  RAND_bytes((unsigned char *)keys, num_ops * sizeof(uint64_t));

  for (int i = 0; i < num_ops; i++) {
    int op_type = rand() % 3;
    int existing = rand() % 2;
    int deleted = rand() % 2;
    uint64_t key;
    uint64_t new_value;
    uint64_t existing_value;

    if (existing && map.size() > 0) {
      key = get_random_key(map, key_bits);
    } else if (deleted && deleted_keys.size() > 0) {
      key = deleted_keys[rand() % deleted_keys.size()];
    } else {
        key = keys[i] & BITMASK(key_bits);
    }

    if (map.find(key) != map.end()) {
        existing_value = map[key];
    } else {
        existing_value = -1;
    }
    new_value = values[i] & BITMASK(value_bits);

    switch (op_type) {
    case INSERT:
      ops.push_back({INSERT, key, new_value});
      map[key] = new_value;
      break;
    case DELETE:
      ops.push_back({DELETE, key, existing_value});
      if (existing_value != -1) {
        deleted_keys.push_back(key);
        map.erase(key);
      }
    case LOOKUP:
      ops.push_back({LOOKUP, key, existing_value});
      break;
    default:
      break;
    }
  }
  delete keys;
  delete values;
}

int key_bits = 16;
int quotient_bits = 8;
int value_bits = 8;
int initial_load_factor = 50;
int num_ops = 200;
bool should_replay = false;
std::string datastruct = "rhm";
std::string replay_file = "test_case.txt";
std::map<uint64_t, uint64_t> current_state;
hashmap hm = rhm;

void check_universe(uint64_t key_bits, std::map<uint64_t, uint64_t> expected, hashmap actual, bool check_equality = false) {
  uint64_t value;
  for (uint64_t k = 0; k < (1UL<<key_bits); k++) {
    int key_exists = expected.find(k) != expected.end();
    int ret = actual.lookup(k, &value);
    if (key_exists) {
      uint64_t expected_value = expected[k];
      assert(ret >= 0);
      if (check_equality) assert(expected_value == value);
    } else {
      assert(ret == QF_DOESNT_EXIST);
    }
  }
}

void usage(char *name) {
  printf("%s [OPTIONS]\n"
         "Options are:\n"
         "  -d datastruct           [ Default rhm. rhm, trhm ]\n"
         "  -k keysize bits         [ log_2 of map capacity.  Default 16 ]\n"
         "  -q quotientbits         [ Default 8. Max 64.]\n"
         "  -v value bits           [ Default 8. Max 64.]\n"
         "  -m initial load factor  [ Initial Load Factor[0-100]. Default 50. ]\n"
         "  -l                      [ Random Ops. Default 50.]\n"
         "  -r replay               [ Whether to replay. If 0, will record to -f ]\n"
         "  -f file                 [ File to record. Default test_case.txt ]\n",
         name);
}

void parseArgs(int argc, char **argv) {
  int opt;
  char *term;

  while ((opt = getopt(argc, argv, "d:k:q:v:m:l:f:r:")) != -1) {
    switch (opt) {
    case 'd':
      datastruct = std::string(optarg);
      break;
    case 'k':
      key_bits = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -n must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'q':
      quotient_bits = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -q must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'v':
      value_bits = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -v must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'm':
      initial_load_factor = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -m must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'l':
      num_ops = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -l must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'r':
        should_replay = strtol(optarg, &term, 10);
        if (*term) {
            fprintf(stderr, "Argument to -r must be an integer (0 to disable) \n");
            usage(argv[0]);
            exit(1);
        }
        break;
    case 'f':
        replay_file = std::string(optarg);
        break;
    }
    if (datastruct == "rhm") {
      hm = rhm;
    } else if (datastruct == "trhm") {
      hm = trhm;
    } else {
      fprintf(stderr, "Argument to -d must one of 'rhm', 'trhm'. \n");
      usage(argv[0]);
      exit(1);
    }
    // TODO(chesetti): Add assertions that flags are sane.
  }
}

int main(int argc, char **argv) {
  parseArgs(argc, argv);
  cout << "Key Bits: " << key_bits << std::endl;
  cout << "Quotient Bits: " << quotient_bits << std::endl;
  cout << "Value Bits: " << value_bits << std::endl;
  cout << "LoadFactor : " << initial_load_factor << std::endl;
  cout << "Num Ops: " << num_ops << std::endl;
  cout << "Is Replay: " << should_replay << std::endl;
  cout << "Test Case Replay File: " << replay_file << std::endl;

  std::vector<hm_op> ops;
  if (should_replay) {
    load_ops(replay_file, &key_bits, &quotient_bits, &value_bits, ops);
  } else {
    generate_ops(key_bits, quotient_bits, value_bits, initial_load_factor,
               num_ops, ops);
  }
  write_ops(replay_file, key_bits, quotient_bits, value_bits, ops);

  std::map<uint64_t, uint64_t> map;
  hm.init((1ULL<<quotient_bits), key_bits, value_bits);
  uint64_t key, value;
  int ret, key_exists;
  for (int i=0; i < ops.size(); i++) {
    auto op = ops[i];
    key = op.key;
    value = op.value;
    #if DEBUG
    printf("%d op: %d, key: %lx, value:%lx.\n", i, op.op, key, value);
    #endif
    switch(op.op) {
      case INSERT:
        map[key] = value;
        ret = hm.insert(key, value);
        if (ret < 0 && ret != QF_KEY_EXISTS) {
          fprintf(stderr, "Insert failed. Replay this testcase with ./test_case -d %s -r 1 -f %s\n", datastruct.c_str(), replay_file.c_str());
          abort();
        }
        check_universe(key_bits, map, hm);
        break;
      case DELETE:
        key_exists = map.erase(key);
        printf("key_exists: %d\n", key_exists);
        ret = hm.remove(key);
        if (key_exists && ret < 0) {
          fprintf(stderr, "Delete failed. Replay this testcase with ./test_case -d %s -r 1 -f %s\n", datastruct.c_str(), replay_file.c_str());
          abort();
        }
        check_universe(key_bits, map, hm);
        break;
      case LOOKUP:
        ret = hm.lookup(key, &value);
        if (map.find(key) != map.end()) {
          if (ret < 0)  {
            fprintf(stderr, "Find failed. Replay this testcase with ./test_case -d %s -r 1 -f %s\n", datastruct.c_str(), replay_file.c_str());
            abort();
          }
        }
        break;
    }
  }
  hm.destroy();
}