import pandas as pd
import matplotlib.pyplot as plt

try:
    df = pd.read_csv('output/csv_files/MonteCarlo_performance_data.csv')
    dfGX = pd.read_csv('output/csv_files/GraphX_performance_data.csv')
    dfConf = pd.read_csv('output/csv_files/MC_configs.csv')
except FileNotFoundError:
    print(f"Error: The data file was not found.")
    print("Please create the file with your performance data.")
    exit()

plt.figure(figsize=(10, 6))

plt.plot(
    df['memory_mb'],
    df['runtime_sec'],
    marker='o',
    linestyle='-',
    color='b',
    label='Monte-Carlo Method'
)

plt.plot(
    dfGX['memory_mb'],
    dfGX['runtime_sec'],
    marker='o',
    linestyle='-',
    color='r',
    label='GraphX Method'
)

numWalkers = dfConf.at[0, 'numWalkers']
numSteps = dfConf.at[0, 'numSteps']
resetProb = dfConf.at[0, 'resetProb']

config_text = f"Num.walkers per node factor: {numWalkers}\n" \
              f"Steps: {numSteps}\n" \
              f"Reset probability: {resetProb}"

plt.text(0.02,
         0.87,
         config_text,
         fontsize=10,
         transform=plt.gca().transAxes,
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='lightgray'))

plt.title('Runtime vs. Executor Memory')
plt.xlabel('Executor Memory per Executor (MB)')
plt.ylabel('Total Runtime (seconds)')

plt.grid(True)

#plt.legend()

output_image_file = 'output/plots/memory_vs_runtime_v4.png'
plt.savefig(output_image_file)
print(f"Plot saved to '{output_image_file}'")
