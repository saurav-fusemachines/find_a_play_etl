import concurrent.futures
# from scraping_script.concord import start_concord
from scraping_script.mti_scraping import start_mti
from scraping_script.trw import start_trw
from cleaning_script.mti_data_cleaning import cleaning_script_main

# task1 = start_concord()
# task2 = start_mti()
# task3 = start_trw()

def run_tasks():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Start the tasks concurrently
        # future1 = executor.submit(start_concord)
        future2 = executor.submit(start_mti)
        # future3 = executor.submit(start_trw)

        # Wait for all tasks to complete
        concurrent.futures.wait([future2])

        future3 = executor.submit(cleaning_script_main)

        concurrent.futures.wait([future3])

# Run the tasks
run_tasks()