#!/usr/bin/python3
import sys
import pandas as pd
from matplotlib import pyplot as plt

num_keys = sys.argv[1]
num_churn_ops = sys.argv[2]

print(num_keys, num_churn_ops)

df = pd.read_csv('rhm-insert.txt', delim_whitespace=True)
plt.plot(df["x_0"], df["y_0"])
plt.xlabel("percent of keys inserted (%d)" % int(num_keys))
plt.ylabel("throughput (num inserts per sec)")
plt.savefig("plot_insert.png")
plt.close()

df = pd.read_csv('rhm-churn.txt', delim_whitespace=True)
plt.plot(df["x_0"], df["y_0"])
plt.xlabel("percent of churn test(%d)" % int(num_churn_ops))
plt.ylabel("throughput (num ops per sec)")
plt.savefig("plot_churn.png")
plt.close()