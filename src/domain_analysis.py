import sys
import json
import matplotlib.pyplot as plt

def get_domain_frequency(filepath, domain_frequency):
    with open(filepath, "r") as f:
        for line in f:
            data = json.loads(line)
            try:
                domain = data["LoadDomain"]
                if domain not in domain_frequency:
                    domain_frequency.setdefault(domain, 1)
                else:
                    domain_frequency[domain] += 1
            except Exception as e:
                print("Exception: " , e)
                continue

def visualize_histogram(domain_frequency, folder):
    domain_frequency = {k: v for k, v in sorted(domain_frequency.items(), key=lambda item: item[1])}
    plt.xticks(rotation='vertical')
    plt.bar(domain_frequency.keys(), domain_frequency.values(), color='g')
    plt.margins(0.2)
    plt.subplots_adjust(bottom=0.5)
    plt.savefig("domain_freq_"+folder+".png")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 domain_analysis.py path/to/file")
        exit()

    filepath = sys.argv[1]
    folder = filepath.split("/")[-1]
    domain_frequency = {}
    get_domain_frequency(filepath, domain_frequency)
    print(domain_frequency)
    visualize_histogram(domain_frequency, folder)
    pass