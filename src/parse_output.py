import glob
import matplotlib.pyplot as plt
import numpy as np

def parse_output(folder_name):
    total = 0
    cdns = {}
    for filename in glob.glob(folder_name+'/unique*'):
        with open(filename) as f:
            lines = f.readlines()
            for line in lines:
                total += 1
                if 'CDN missing' in line:
                    cdns.setdefault('CDN missing', 0)
                    cdns['CDN missing'] += 1
                else:
                    line = line.split('}')
                    cdns_present = line[-2].split('{')[-1].split(',')
                    for cdn_p in cdns_present:
                        cdns.setdefault(cdn_p, 0)
                        cdns[cdn_p] += 1
    return cdns, total

def dict_to_arr(dictionary, threshold):
    titles = []
    frequency = []
    for key in dictionary:
        if dictionary[key] < threshold:
            continue
        titles.append(key)
        frequency.append(dictionary[key])
    return titles, frequency

def plot_histogram_horizontal(titles, freq, fig_name, y_label, x_label, title):
    total = np.sum(freq)
    max_count = np.max(freq)
    plt.figure(figsize=(10, 20))
    y_pos = np.arange(len(titles))
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.barh(y_pos, freq, facecolor='green', alpha=0.5, orientation="horizontal", linewidth=1)   
    for i in range(len(freq)): 
        plt.text(freq[i], y_pos[i], str(freq[i]))
    plt.yticks(y_pos, titles)
    plt.savefig(fig_name, bbox_inches="tight", pad_inches=0.5)
    plt.close()
    print(total, max_count)
    
if __name__ == "__main__":
    cdns, total = parse_output('../output')
    print(total)
    titles, freq = dict_to_arr(cdns, 500)
    plot_histogram_horizontal(titles, freq, 'CDNfreq.png', 'CDN types', 'Frequency', 'CDNs associated with IPs of unique backward mapping (Frequency > 500)')


