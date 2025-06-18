import pandas as pd
import matplotlib.pyplot as plt

data_file = 'MonteCarlo_performance_data.csv'

try:
    df = pd.read_csv(data_file)
    dfGX = pd.read_csv('GraphX_performance_data.csv')
except FileNotFoundError:
    print(f"Error: The data file '{data_file}' was not found.")
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

plt.text(400, 70, 'NumWalkers: 10\n'
                  'NumSteps: 30\n'
                  'ResetProb: 0.15',
         fontsize=10, bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='lightgray'))

plt.title('Runtime vs. Executor Memory')
plt.xlabel('Executor Memory per Executor (MB)')
plt.ylabel('Total Runtime (seconds)')

plt.grid(True)

plt.legend()

output_image_file = 'memory_vs_runtime_v1.png'
plt.savefig(output_image_file)
print(f"Plot saved to '{output_image_file}'")
