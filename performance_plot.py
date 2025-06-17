import pandas as pd
import matplotlib.pyplot as plt

# 1. Define the input data file
data_file = 'MonteCarlo_performance_data.csv'

# 2. Read the data using pandas
try:
    df = pd.read_csv(data_file)
    dfGX = pd.read_csv('GraphX_performance_data.csv')
except FileNotFoundError:
    print(f"Error: The data file '{data_file}' was not found.")
    print("Please create the file with your performance data.")
    exit()

# 3. Create the plot
plt.figure(figsize=(10, 6))  # Set the figure size for better readability

plt.plot(
    df['memory_mb'],  # X-axis data
    df['runtime_sec'],  # Y-axis data
    marker='o',  # Add circles at each data point
    linestyle='-',  # Connect points with a solid line
    color='b',  # Set line color to blue
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

# 4. Add labels and a title for clarity
plt.title('Runtime vs. Executor Memory')
plt.xlabel('Executor Memory per Executor (MB)')
plt.ylabel('Total Runtime (seconds)')

# 5. Add a grid for easier value reading
plt.grid(True)

# 6. Save the plot to an image file
output_image_file = 'memory_vs_runtime.png'
plt.savefig(output_image_file)
print(f"Plot saved to '{output_image_file}'")

# 7. Display the plot
plt.show()
