#!/bin/bash

# Run instructions
# sh run_experiment.sh ...workload_types

# Move to the root of the repository.
scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir/../.."

# Prologue
workloads=("$@") # array of string inputs such as "ycsb", "tpcc"
target_file_path="target/benchbase-mysql"

# declare -a terminals=("1", "2", "10", "50", "100")
declare -a terminals=("1" "2" "10" "50" "100")


# Run the experiment for a particular workload for various numbers of clients/terminals
for workload in "${workloads[@]}"; do
    echo "INFO: Running experiments for $workload"
    config_file="sample_${workload}_config.xml"
    for t_num in "${terminals[@]}"; do
        # Update the config file with the number of terminals
        echo "INFO: Setting number of terminals to $t_num for $workload"
        config_file_path="$target_file_path/config/mysql/$config_file"
        sed -i '' "s|<terminals>.*</terminals>|<terminals>$t_num</terminals>|g" "$config_file_path"

        # Run the experiment and store terminal output into a file
        echo "INFO: Running experiment for $t_num terminals for $workload"
        benchbase_jar="$target_file_path/benchbase.jar"
        time_string=$(date +"%m-%d_%H-%M-%S")
        rm -rf results/* # CLEAR "results" FOLDER -> PLEASE CHANGE THIS IF YOU WANT TO KEEP THE OTHER FILES AS WELL
        output_folder_path="experiments/results/$workload/$t_num:$time_string" # file name format: <terminals>:<time_string>
        mkdir -p "$(dirname "$output_folder_path/terminal_output.txt")"
        java -jar $benchbase_jar -b ycsb -c $config_file_path --create=true --load=true --execute=true &> "$output_folder_path/terminal_output.txt"

        # Compile the results
        echo "INFO: Done running experiment for $t_num terminals for $workload"
        echo "INFO: Analyzing result for $t_num terminals for $workload"
        cp results/*summary.json "$output_folder_path"
    done
done

exit 0
