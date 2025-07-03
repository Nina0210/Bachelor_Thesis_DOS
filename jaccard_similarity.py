import csv
import matplotlib.pyplot as plt
import pandas as pd


def csv_to_list(file):
    first_column = []
    with open(file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            if row:
                first_column.append(row[0])
    # print(first_column)
    return first_column


def get_walkers(file):
    # second_column = []
    with open(file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            if row:
                walkers_per_node = row[0]
    return walkers_per_node


def jaccard(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    similarity = float(intersection) / union
    distance = 1 - similarity
    return similarity, distance


GX_ranks_list = csv_to_list('output/csv_files/GX_top_20_ranks_1000m.csv')
# GX_ranks_list = csv_to_list('output/csv_files/run_2025-06-19_15-45-23/MC_top_20_ranks_15_1000m.csv')

all_values = []
avg_and_walker_values = []

for j in range(1, 6):
    print(f"Calculating average Jaccard similarity for run {j} ...")
    for i in range(1, 6):
        MC_ranks_list = csv_to_list(f'output/csv_files/run_{j}/MC_top_20_ranks_{i}_1000m.csv')
        J_similarity, J_Distance = jaccard(GX_ranks_list, MC_ranks_list)
        all_values.append(J_similarity)
        print(f"--- Values for {i}th loop --- ")
        print(f"Jaccard Similarity: {J_similarity}")
        print(f"Jaccard DIstance: {J_Distance}")
    avg = sum(all_values) / len(all_values)
    steps = get_walkers(f"output/csv_files/run_{j}/MC_configs.csv")
    avg_and_walker_values.append((steps, avg))
    print(f"\nAverage Jaccard similarity run {j}: {avg}")


print(f"walkers and average list: {avg_and_walker_values}")
walkers = [int(step) for step, _ in avg_and_walker_values]
averages = [avg for _, avg in avg_and_walker_values]

# plt.bar(walkers, averages)
# plt.xlabel("Walkers per Node")
# plt.ylabel("Jaccard Similarity")
# plt.title("Walkers vs. Jaccard Similarity")
# plt.xticks(walkers)  # Set ticks to exact natural numbers

plt.plot(walkers, averages, linestyle='--', marker='o')
plt.xlabel('Walkers per Node')
plt.ylabel('Jaccard Similarity')
plt.title('Walkers vs. Jaccard Similarity')
plt.grid(True)
plt.tight_layout()
#plt.vlines(walkers, [0], averages, colors='gray', linestyles='dotted', linewidth=1)
#plt.legend()

try:
    df = pd.read_csv('output/csv_files/run_3/MC_configs.csv')
except FileNotFoundError:
    print("Error: The data file was not found.")
    print("Please create the file with your performance data.")
    exit()

steps = df.at[0, 'numSteps']
reset_prob = df.at[0, 'resetProb']

config_text = f"Steps: {steps}\n" \
              f"Reset probability: {reset_prob}"

plt.text(0.015,
         0.89,
         config_text,
         fontsize=10,
         transform=plt.gca().transAxes,
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='lightgray'))

output_image_file = 'output/plots/steps_vs_accuracy_server.png'
plt.savefig(output_image_file)
print(f"Plot saved to '{output_image_file}'")

# save to csv for different step numbers and plot graph x axis: number steps, y axis: avg similarity
