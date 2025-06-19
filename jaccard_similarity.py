import csv


def csv_to_list(file):
    first_column = []
    with open(file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            if row:
                first_column.append(row[0])
    #print(first_column)
    return first_column


def jaccard(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    similarity = float(intersection) / union
    distance = 1 - similarity
    return similarity, distance


GX_ranks_list = csv_to_list('output/csv_files/GX_top_20_ranks_1000m.csv')

all_values = []

for i in range(1, 11):
    MC_ranks_list = csv_to_list(f'output/csv_files/MC_top_20_ranks_{i}_1000m.csv')
    J_similarity, J_Distance = jaccard(GX_ranks_list, MC_ranks_list)
    all_values.append(J_similarity)
    print(f"--- Values for {i}th loop --- ")
    print(f"Jaccard Similarity: {J_similarity}")
    print(f"Jaccard DIstance: {J_Distance}")

avg = sum(all_values) / len(all_values)
print(f"\nAverage Jaccard similarity: {avg}")