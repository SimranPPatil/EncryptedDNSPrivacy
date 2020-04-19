import sys
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("python3 file_for_hist xlabel ylabel title figname")
        exit()

    cnts = []
    sizes = []
    file_name = sys.argv[1]
    xlabel = sys.argv[2]
    ylabel = sys.argv[3]
    title = sys.argv[4]
    figname = sys.argv[5]
    with open(file_name) as f:
        for line in f:
            if "Row" in line:
                line = line.split(")")[0].split("((")[1].split(", ")
                cnt = int(line[0])
                size = int(line[1])
                cnts.append(cnt)
                sizes.append(size)

    print(len(cnts), len(sizes))
    plt.bar(cnts[:100], np.log(sizes)[:100])
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.savefig(figname)

