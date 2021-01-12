import string
import argparse
import pandas as pd
import numpy as np
from numpy import random


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--f', type=int, default=2,
                        help='Number of Features (Columns)')
    parser.add_argument('--s', type=int, default=1000,
                        help='Dataset Size (Rows)')
    args = parser.parse_args()
    distribute(args)


def distribute(args):
    n = args.s
    feats = args.f
    if n >= 10**6:
        csv_name = f"{int(n/10**6)}M"
    else:
        csv_name = f"{int(n/10**3)}K"

    base = np.random.normal(loc=.5, scale=.5, size=n)
    correlated = []
    epsilons = [np.random.normal(loc=.5, scale=.5, size=n) for _ in range(feats)]
    labels = list(string.ascii_lowercase.upper()[0:10])
    uniform = random.uniform(size=(feats, n))
    normal = random.normal(loc=.5, scale=5, size=(feats, n))
    chisquare = random.chisquare(df=feats, size=(feats, n))
    for index, i in enumerate(epsilons):
        if index == 0:
            correlated.append(base)
        else:
            correlated.append(base + random.uniform(0.5, 1) * i)
    for index, x in enumerate(correlated):
        correlated[index] = (x - min(x)) / (max(x) - min(x))
    for index, x in enumerate(normal):
        normal[index] = (x - min(x)) / (max(x) - min(x))
    for index, x in enumerate(chisquare):
        chisquare[index] = (x - min(x)) / (max(x) - min(x))

    uniform_df = pd.DataFrame(data=uniform.T, columns=labels[0:feats])
    normal_df = pd.DataFrame(data=normal.T, columns=labels[0:feats])
    chisquare_df = pd.DataFrame(data=chisquare.T, columns=labels[0:feats])
    correlated_df = pd.DataFrame(data=np.array(correlated).T, columns=labels[0:feats])

    uniform_df.to_csv(f"{csv_name}x{feats}_uniform.csv", index=False)
    normal_df.to_csv(f"{csv_name}x{feats}_normal.csv", index=False)
    chisquare_df.to_csv(f"{csv_name}x{feats}_chisquare.csv", index=False)
    correlated_df.to_csv(f"{csv_name}x{feats}_correlated.csv", index=False)


if __name__ == '__main__':
    main()
