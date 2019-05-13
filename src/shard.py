with open("../input/1m-resources.csv", "r") as fp:
    for line in fp:
        row = line.split(',')
        idx = int(row[0])%100
        print(idx)
        fname = "shards/out_" + str(idx) + ".csv"
        with open(fname, "a+") as outfile:
            outfile.write(line) 

