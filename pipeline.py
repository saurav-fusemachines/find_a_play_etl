import concurrent.futures
from scraping_script.concord import start_concord
from scraping_script.mti_scraping import start_mti
from scraping_script.trw import start_trw
from cleaning_script.mti_data_cleaning import cleaning_script_main as mti_cleaning
from cleaning_script.trw_data_cleaning import cleaning_trw_main as trw_cleaning
from cleaning_script.concord_cleaning import cleaning_script_main as concord_cleaning
# task1 = start_concord()
# task2 = start_mti()
# task3 = start_trw()

def run_tasks():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Start the tasks concurrently
        scraping_concord = executor.submit(start_concord)
        scraping_mti = executor.submit(start_mti)
        scraping_trw = executor.submit(start_trw)

        # Wait for all tasks to complete
        concurrent.futures.wait([scraping_concord, scraping_mti, scraping_trw])

        cleaning_mti = executor.submit(mti_cleaning)
        cleaning_trw = executor.submit(trw_cleaning)
        cleaning_concord = executor.submit(concord_cleaning)

        concurrent.futures.wait([cleaning_mti, cleaning_trw, cleaning_concord])
        
# Run the tasks
run_tasks()