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


def get_steps(file):
    # second_column = []
    with open(file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            if row:
                steps = row[1]
    return steps


def jaccard(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    similarity = float(intersection) / union
    distance = 1 - similarity
    return similarity, distance


GX_ranks_list = csv_to_list('output/csv_files/GX_top_20_ranks_1000m.csv')
# GX_ranks_list = csv_to_list('output/csv_files/run_2025-06-19_15-45-23/MC_top_20_ranks_15_1000m.csv')

all_values = []
avg_and_steps_values = []

for j in range(1, 8):
    print(f"Calculating average Jaccard similarity for run {j} ...")
    for i in range(1, 31):
        MC_ranks_list = csv_to_list(f'output/csv_files/run_{j}/MC_top_20_ranks_{i}_1000m.csv')
        J_similarity, J_Distance = jaccard(GX_ranks_list, MC_ranks_list)
        all_values.append(J_similarity)
        print(f"--- Values for {i}th loop --- ")
        print(f"Jaccard Similarity: {J_similarity}")
        print(f"Jaccard DIstance: {J_Distance}")
    avg = sum(all_values) / len(all_values)
    steps = get_steps(f"output/csv_files/run_{j}/MC_configs.csv")
    avg_and_steps_values.append((steps, avg))
    print(f"\nAverage Jaccard similarity run {j}: {avg}")


print(f"steps and average list: {avg_and_steps_values}")
steps = [int(step) for step, _ in avg_and_steps_values]
averages = [avg for _, avg in avg_and_steps_values]

plt.plot(steps, averages, marker='o')
plt.xlabel('Steps')
plt.ylabel('Jaccard Similarity')
plt.title('Steps vs. Jaccard Similarity')
plt.grid(True)
plt.tight_layout()
#plt.legend()

try:
    df = pd.read_csv('output/csv_files/run_1/MC_configs.csv')
except FileNotFoundError:
    print("Error: The data file was not found.")
    print("Please create the file with your performance data.")
    exit()

num_walkers_per_node = df.at[0, 'numWalkers']
reset_prob = df.at[0, 'resetProb']

config_text = f"Num.walkers per node factor: {num_walkers_per_node}\n" \
              f"Reset probability: {reset_prob}"

plt.text(0.015,
         0.025,
         config_text,
         fontsize=10,
         transform=plt.gca().transAxes,
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='lightgray'))

output_image_file = 'output/plots/steps_vs_accuracy_v1.png'
plt.savefig(output_image_file)
print(f"Plot saved to '{output_image_file}'")

# save to csv for different step numbers and plot graph x axis: number steps, y axis: avg similarity
