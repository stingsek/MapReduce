from mrjob.job import MRJob
from file_utils import convert_xlsx_to_csv, save_to_file
from MRJobSzkolyWojewodztwa import MRJobRok_LiczbaSzkół, MRJobRok_LiczbaUczniow, MRJobTypObszaru_Zawody_LiczbaUczniow, MRJobUczniowie_w_Zawodach, MRJobSzkoly_TypPodmiotu_Wojewodztwa, MRJobSzkoly_Wojewodztwa, MRJobWojewodztwo_LiczbaUczniow, MRJobSzkoly_Publicznosc_Wojewodztwa
import subprocess
import time
import os
from utils import generate_stats_report

def run_mrjob(job_class, input_path, runner_type):
    """Uruchamia MRJob i zapisuje wynik do pliku JSON + loguje czas."""
    start_time = time.time()
    results = []

    info_line = f"{job_class.__name__} na pliku: {input_path} (runner: {runner_type})"
    print(f"[RUNNER] Startuję MRJob: {info_line}")

    mr_job = job_class(args=[input_path, f'--runner={runner_type}', '--verbose'])

    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.cat_output():
            for key, value in mr_job.parse_output([line]):
                results.append([key, value])

    duration = time.time() - start_time
    
    input_suffix = os.path.basename(input_path)[-12:-4]
    job_name = job_class.__name__[5:]
    output_path = f"results/{input_suffix}/{job_name}_{input_suffix}.json"

    save_to_file(results, output_path)

    log_time(info_line, duration)
    generate_stats_report(results, output_path)


def log_time(info_line, duration):
    """Zapisuje czas trwania joba do pliku logów."""
    with open("czas_log.txt", "a", encoding="utf-8") as f:
        f.write(f"{info_line}\n")
        f.write(f"Czas trwania: {duration:.2f} sekund\n\n")


def run_hadoop_streaming_and_log(input_hdfs_path, mapper_script):
    """Uruchamia Hadoop Streaming i wypisuje statystyki z logów joba."""
    command = f"""
    echo "test line" | hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input {input_hdfs_path} \
        -output /user/agata/test-output \
        -mapper "python3 {mapper_script}" \
        -reducer NONE \
        -files {mapper_script}
    """

    process = subprocess.run(command, shell=True, capture_output=True, text=True)

    print("[STDERR — LOGI HADOOP]:")
    for line in process.stderr.splitlines():
        if any(keyword in line for keyword in [
            "Launched map tasks",
            "Data-local map tasks",
            "Map input records",
            "Map output records",
            "Job",
            "Counters:"
        ]):
            print(line)

    print("[STDOUT]:")
    print(process.stdout)


def runJobs(csv_path=None):

    print("=== START ===")

    # csv_file_path2324_short = "data/uczniowie20232026.csv"

    path2324Hdfs = "hdfs:///user/agata/input/uczniowie20232024.csv"
    # runner_type = "hadoop"
    runner_type = "inline"
    # runner_type = "local"

    run_mrjob(MRJobUczniowie_w_Zawodach, csv_path, runner_type) #1
    # run_mrjob(MRJobTypObszaru_Zawody_LiczbaUczniow, csv_path, runner_type) #2
    # run_mrjob(MRJobSzkoly_Wojewodztwa, csv_path, runner_type) #3
    # run_mrjob(MRJobSzkoly_TypPodmiotu_Wojewodztwa, csv_path, runner_type) #4
    # run_mrjob(MRJobSzkoly_Publicznosc_Wojewodztwa, csv_path, runner_type) #5
    # run_mrjob(MRJobWojewodztwo_LiczbaUczniow, csv_path, runner_type) #6
    # run_mrjob(MRJobRok_LiczbaUczniow, csv_path, runner_type) #7
    # run_mrjob(MRJobRok_LiczbaSzkół, csv_path, runner_type) #8


def full_files():
    csv_file_path2223 = "data/processed_uczniowie20222023.csv"
    csv_file_path2324 = "data/processed_uczniowie20232024.csv"
    csv_file_path2425 = "data/processed_uczniowie20242025.csv"

    runJobs(csv_file_path2223)
    runJobs(csv_file_path2324)
    runJobs(csv_file_path2425)

if __name__ == '__main__':
    full_files()
