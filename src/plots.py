import sys
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("python3 file_for_hist")
        exit()

    cnts = []
    sizes = []
    file_name = sys.argv[1]
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
    plt.xlabel("ASN Anonymity set size")
    plt.ylabel("Log of number of sites with given anonymity set size")
    plt.title("ASN anonymity set sizes for sites")
    plt.savefig("asn_anonset.png")

