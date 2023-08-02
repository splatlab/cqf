/*
 * ============================================================================
 *
 *        Authors:  Prashant Pandey <ppandey@cs.stonybrook.edu>
 *                  Rob Johnson <robj@vmware.com>
 *
 * ============================================================================
 */

#include <assert.h>
#include <iostream>
#include <map>
#include <math.h>
#include <openssl/rand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <iostream>
using namespace std;

#include "include/rhm_wrapper.h"
#include "include/trhm_wrapper.h"
#include "include/zipf.h"

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

uint64_t tv2msec(struct timeval tv) {
  uint64_t ret = tv.tv_sec * 1000 + tv.tv_usec / 1000;
  return ret;
}

void usage(char *name) {
  // TODO: Remove the flags not needed.
  printf("%s [OPTIONS]\n"
         "Options are:\n"
         "  -n nslots       [ log_2 of map capacity.  Default 22 ]\n"
         "  -r nruns        [ number of runs.  Default 1 ]\n"
         "  -c churn cycles [ number of churn cycles.  Default 10 ]\n"
         "  -l churn length [ Number of insert, delete and query operations "
         "per churn. Default 500. ]\n"
         "  -p npoints    [ number of points on the graph.  Default 20 ]\n"
         "  -d datastruct  [ Default rhm. ]\n"
         "  -f outputfile  [ default rhm. ]\n",
         "  -y replay \n",
         name);
}

struct hm_op {
  int op;
  uint64_t key;
  uint64_t value;
};

std::vector<hm_op> load_ops() {
  std::vector<hm_op> ops;
  ifstream replay_file;
  replay_file.open("replay.txt");
  uint64_t num_keys;
  replay_file >> num_keys;
  for (int i=0; i<num_keys; i++) {
    hm_op op;
    replay_file >> op.op >> op.key >> op.value;
    ops.push_back(op);
  }
  replay_file.close();
  return ops;
}

std::vector<hm_op> write_ops(std::vector<hm_op> &ops) {
  ofstream replay_file;
  replay_file.open("replay.txt");
  replay_file << ops.size() << std::endl;
  for (auto op : ops) {
    replay_file << op.op <<" "<<op.key<<" "<<op.value<<std::endl;
  }
  replay_file.close();
}

std::vector<hm_op> generate_ops(uint64_t num_preload_keys, int nchurns,
                                int churn_len, int q_mul) {
  printf("Generating OPS\n");
  std::unordered_map<uint64_t, uint64_t> hm;
  std::vector<uint64_t> hm_kv;
  std::vector<hm_op> ops;
  ops.reserve(num_preload_keys + churn_len * nchurns * 3);

  uint64_t *kv = new uint64_t[2 * num_preload_keys];
  RAND_bytes((unsigned char *)kv, sizeof(uint64_t) * 2 * num_preload_keys);

  for (uint64_t i = 0; i < 2 * num_preload_keys; i += 2) {
    uint64_t key = (kv[i] & BITMASK(32));
    uint64_t value = kv[i + 1] & BITMASK(32);
    hm_kv.push_back(key<<32 | value);
    hm[key] = value;
    ops.push_back(hm_op{0, key, value});
  }
  uint64_t *ci_kv = new uint64_t[churn_len]; // Churn Insert Key Values.
  uint64_t *cd_idx = new uint64_t[churn_len];    // Churn Insert Key Values.
  uint64_t *cq_idx = new uint64_t[q_mul * churn_len];    // Churn Insert Key Values.
  for (int i = 0; i < nchurns; i++) {
    // DELETE EXISTING KEYS
    RAND_bytes((unsigned char *)cd_idx, sizeof(uint64_t) * churn_len);
    for (int i = 0; i < churn_len; i++) {
      uint32_t rand_idx = cd_idx[i] % hm_kv.size();
      uint64_t kv = hm_kv[rand_idx];
      uint64_t key = kv >> 32;
      if (hm.find(key) != hm.end()) {
        uint64_t val = hm[key];
        hm.erase(key);
        ops.push_back(hm_op{1, key, val});
      }
    }
    // INSERT NEW KEYS
    RAND_bytes((unsigned char *)ci_kv, sizeof(uint64_t) * churn_len);
    for (int i = 0; i < churn_len; i++) {
      uint32_t rand_idx = cd_idx[i] % hm_kv.size();
      // Insert the key into the slot that was just deleted.
      uint64_t kv = ci_kv[i];
      uint64_t key = kv >> 32;
      uint64_t val = kv & BITMASK(32);
      hm_kv[rand_idx] = kv;
      hm[key] = val;
      ops.push_back(hm_op{0, key, val});
    }
    // LOOKUP EXISTING KEYS
    RAND_bytes((unsigned char *)cq_idx, sizeof(uint64_t) * churn_len * q_mul);
    for (int i = 0; i < churn_len * q_mul; i++) {
      uint32_t rand_idx = cq_idx[i] % hm_kv.size();
      uint64_t kv = hm_kv[rand_idx];
      uint64_t key = kv >> 32;
      if (hm.find(key) != hm.end()) {
        ops.push_back(hm_op{2, key, hm[key]});
      }
    }
  }
  delete ci_kv;
  delete cd_idx;
  delete kv;
  printf("DOne Generating OPS\n");
  return ops;
}

int main(int argc, char **argv) {
  uint32_t nbits = 22, nruns = 1;
  int nchurns = 10;
  int nchurn_ops = 500;
  unsigned int npoints = 20;
  uint64_t nslots = (1ULL << nbits), nkeys = 950 * nslots / 1000;
  uint64_t numkeys = 0;
  char *datastruct = "rhm";
  char *outputfile = "rhm";
  char *inputfile = "rhm";
  uint64_t num_inserts=0, num_lookups=0, num_removes=0;

  hashmap hashmap_ds;

  unsigned int i, j, m, exp, run, op_idx;
  struct timeval tv_insert[500][10];
  struct timeval tv_churn[500][10];

  const char *dir = "./";
  const char *insert_op = "-insert.txt\0";
  const char *churn_op = "-churn.txt\0";
  char filename_insert[256];
  char filename_churn[256];

  /* Argument parsing */
  int opt;
  int replay = 0;
  char *term;

  while ((opt = getopt(argc, argv, "n:r:p:m:d:a:f:i:v:s:c:l:y:")) != -1) {
    switch (opt) {
    case 'n':
      nbits = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -n must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      nslots = (1ULL << nbits);
      nkeys = 950 * nslots / 1000;
      break;
    case 'r':
      nruns = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -r must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'c':
      nchurns = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -c must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'l':
      nchurn_ops = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -l must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'p':
      npoints = strtol(optarg, &term, 10);
      if (*term) {
        fprintf(stderr, "Argument to -p must be an integer\n");
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'd':
      datastruct = optarg;
      break;
    case 'f':
      outputfile = optarg;
      break;
    case 'i':
      inputfile = optarg;
      break;
    case 'v':
      numkeys = (int)strtol(optarg, &term, 10);
      break;
    case 'y':
      replay = (int)strtol(optarg, &term, 10);
      break;
    default:
      fprintf(stderr, "Unknown option\n");
      usage(argv[0]);
      exit(1);
      break;
    }
  }
  if (strcmp(datastruct, "rhm") == 0) {
    hashmap_ds = rhm;
  } else if (strcmp(datastruct, "trhm") == 0) {
    hashmap_ds = trhm;
    //	} else if (strcmp(datastruct, "cf") == 0) {
    //		filter_ds = cf;
    //	} else if (strcmp(datastruct, "bf") == 0) {
    //		filter_ds = bf;
  } else {
    fprintf(stderr, "Unknown datastruct.\n");
    usage(argv[0]);
    exit(1);
  }

  snprintf(filename_insert,
           strlen(datastruct) + strlen(outputfile) + strlen(insert_op) + 1, "%s%s%s",
           datastruct, outputfile, insert_op);
  snprintf(filename_churn,
           strlen(datastruct) + strlen(outputfile) + strlen(churn_op) + 1, "%s%s%s",
           datastruct, outputfile, churn_op);

  FILE *fp_insert = fopen(filename_insert, "w");
  FILE *fp_churn = fopen(filename_churn, "w");

  if (fp_insert == NULL || fp_churn == NULL) {
    printf("Can't open the data file");
    exit(1);
  }

  printf("Keys to insert: %lu\n", nkeys);
  for (run = 0; run < nruns; run++) {
    hashmap_ds.init(nslots, 32, 32);
    std::vector<hm_op> ops;
    if (replay) {
      ops = load_ops();
    } else {
        ops = generate_ops(nkeys, nchurns, nchurn_ops, 1);
        write_ops(ops);
    }
    sleep(5);

    int rebuild_freq = 5;
    int num_ops = 0;
    for (exp = 0; exp < 2 * npoints; exp += 2) {
      i = (exp / 2) * (nkeys / npoints);
      j = ((exp / 2) + 1) * (nkeys / npoints);
      printf("Round: %d [%d %d]\n", exp / 2, i, j);

      gettimeofday(&tv_insert[exp][run], NULL);
      uint64_t lookup_value;
      for (op_idx = i; op_idx < j; op_idx++) {
        auto op = ops[op_idx];
        printf("%d %lx %lx\n", op.op, op.key, op.value);
        int ret = hashmap_ds.insert(op.key, op.value);
        #if STRICT
        uint64_t lookup_value = 0;
        ret = hashmap_ds.lookup(op.key, &lookup_value);
        if (ret < 0 || lookup_value != op.value) {
          abort();
        }
        #endif
      }
      gettimeofday(&tv_insert[exp + 1][run], NULL);
    }
    for (; j < nkeys; j++) {
      if (!num_ops) {
        num_ops = rebuild_freq;
        // hashmap_ds.rebuild();
      }
      num_ops--;
      auto op = ops[j];
      printf("%d %lx %lx\n", op.op, op.key, op.value);
      int ret = hashmap_ds.insert(op.key, op.value);
      #if STRICT
      if (ret < 0)
        exit(0);
      #endif
    }

    int churn_ops = ops.size() - nkeys;
    printf("CHURN OPS: %d\n", churn_ops);
    for (exp = 0; exp < 2 * npoints; exp += 2) {
      i = (exp / 2) * (churn_ops / npoints);
      j = ((exp / 2) + 1) * (churn_ops / npoints);
      printf("Round: %d [%d %d]\n", exp / 2, i, j);

      gettimeofday(&tv_churn[exp][run], NULL);
      uint64_t lookup_value;
      int ret;
      for (int op_idx = i; op_idx < j; op_idx++) {
        auto op = ops[op_idx + nkeys]; // nkeys is number of insert keys.
        printf("%d %lx %lx\n", op.op, op.key, op.value);
        switch (op.op) {
        case 0:
          ret = hashmap_ds.insert(op.key, op.value);
          num_inserts++;
          #if STRICT
          if (ret < 0) {
            if (ret == QF_NO_SPACE) {
              printf("NO_SPACE!\n");
              exit(0);
            }
              printf("Insert Failed\n");
            // qf_dump_long(&g_robinhood_hashmap);
            fflush(stdout);
            exit(0);
          }
          #endif
          break;
        case 1:
          ret = hashmap_ds.remove(op.key);
          num_removes++;
          #if STRICT
          if (ret < 0) {
            printf("REMOVE_FAILED\n");
            fflush(stdout);
            exit(0);
          }
          #endif
          break;
        case 2:
          lookup_value = 0;
          ret = hashmap_ds.lookup(op.key, &lookup_value);
          num_lookups++;
          #if STRICT
          if (ret < 0 || lookup_value != op.value) {
            printf("%d LOOKUP_FAILED %lx %lx %lx\n", ret, op.key, op.value, lookup_value);
            fflush(stdout);
            exit(0);
          }
          #endif
          break;
        }
      }
      gettimeofday(&tv_churn[exp + 1][run], NULL);
    }
    hashmap_ds.destroy();
  }

  printf("Writing results to file: %s\n", filename_insert);
  fprintf(fp_insert, "x_0");
  for (run = 0; run < nruns; run++) {
    fprintf(fp_insert, "    y_%d", run);
  }
  fprintf(fp_insert, "\n");
  for (exp = 0; exp < 2 * npoints; exp += 2) {
    fprintf(fp_insert, "%f", ((exp / 2.0) * (100.0 / npoints)));
    for (run = 0; run < nruns; run++) {
      fprintf(fp_insert, " %f",
              0.001 * (nkeys / npoints) /
                  (tv2msec(tv_insert[exp + 1][run]) -
                   tv2msec(tv_insert[exp][run])));
    }
    fprintf(fp_insert, "\n");
  }
  printf("Insert Performance written\n");

  printf("Wiring results to file: %s\n", filename_churn);
  fprintf(fp_churn, "x_0");
  for (run = 0; run < nruns; run++) {
    fprintf(fp_churn, "    y_%d", run);
  }
  fprintf(fp_churn, "\n");
  for (exp = 0; exp < 2 * npoints; exp += 2) {
    fprintf(fp_churn, "%f", ((exp / 2.0) * (100.0 / npoints)));
    for (run = 0; run < nruns; run++) {
      fprintf(
          fp_churn, " %f",
          0.001 * ((nchurns * nchurn_ops * 12) / npoints) /
              (tv2msec(tv_churn[exp + 1][run]) - tv2msec(tv_churn[exp][run])));
    }
    fprintf(fp_churn, "\n");
  }
  uint64_t num_churn_ops = num_lookups + num_inserts + num_removes;
  printf("Churn Performance written\n");
  printf("Keys inserted: %lu\n", nkeys);
  printf("Churn Ops: %lu %lu %lu: %lu\n", num_lookups, num_inserts, num_removes, num_churn_ops);


  fclose(fp_insert);
  fclose(fp_churn);

  char plot_graph[100];
  snprintf(plot_graph, 100, "./plot_graph.py %lu %lu", nkeys, num_churn_ops);
  printf("%s", plot_graph);
  system(plot_graph);

  return 0;
}
