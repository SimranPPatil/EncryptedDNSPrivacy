import sys
import matplotlib.pyplot as plt
from os import walk

def plot_file(filename):
        with open(filename) as curr_file:
    pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("python3 graph path_to_sql_output")
        exit()

    out_dir = sys.argv[1]
    f = []
    for (dirpath, dirnames, filenames) in walk(mypath):
        f.extend(filenames)
        break
        
    pass