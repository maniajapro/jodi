import os, requests, time, csv, json, sys, platform
from requests.auth import HTTPBasicAuth
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from glob import glob
from selenium.webdriver.support import expected_conditions as EC
import psycopg2
import pandas as pd

# get local directory path
DIR_PATH = os.path.abspath(os.path.dirname(__file__))
FOLDER_NAME = '_Output'
FILE_NAME = 'jodi_data.csv'

def file_download(DIR_PATH, FOLDER_NAME):
    # source url
    URL = 'http://www.jodidb.org/TableViewer/tableView.aspx?ReportId=93906'

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.5359.71 Safari/537.36")
    #specify chrome driver path to access site
    options = webdriver.ChromeOptions()
    # download file in specific folder
    prefs = {"download.default_directory": os.path.join(DIR_PATH, FOLDER_NAME),"safebrowsing.enabled": False}
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    driver.get(URL)
    #maximize chrome browser
    driver.maximize_window()
    actions = webdriver.ActionChains(driver)

    #XPATH to click download button
    element_hover=driver.find_element(By.XPATH,'//*[@id="ActDiv"]/table/tbody/tr/td[5]/a/img')
    # move the cursor to the element and click                        
    actions.move_to_element(element_hover).click().perform()
    time.sleep(3)
    # Click and download full csv dataset
    element_download=driver.find_element(By.XPATH,'//*[@id="MenuCell_DownloadDiv"]/p[3]/nobr/a')
    actions.move_to_element(element_download).click().perform()
    driver.switch_to.window(driver.window_handles[1])
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '/html/body/form/table/tbody/tr[1]/td[2]/input[1]')))
    download = driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[1]/td[2]/input[1]").click()
    time.sleep(5)
    driver.quit()
    
    #Rename file name
    file = glob(os.path.join(DIR_PATH,FOLDER_NAME,"world_primary_*"))
    old_name = file[0]
    new_name = os.path.join(DIR_PATH,FOLDER_NAME,FILE_NAME)
    Filename=os.rename(old_name, os.path.join(new_name))

def process_file(DIR_PATH,FOLDER_NAME,FILE_NAME):
    
    list=os.listdir(DIR_PATH)
    time_sorted_list = sorted(list, key=os.path.getmtime)
    file_name = os.path.join(DIR_PATH,FOLDER_NAME,FILE_NAME)
    df = pd.read_csv(file_name, header=3)
    df.drop(0,axis=0, inplace=True)
    melted_df = df.melt(id_vars=['Time'], value_vars=df.loc['Albania':'Yemen'], var_name='Country',value_name='Value')
    melted_df.rename(columns = {'Time':'Country','Country':'Time'}, inplace = True)
    melted_df.rename(columns = {'Country':'country','Time':'month_year','Value':'value'}, inplace = True)
    # Convert the 'date' column to datetime format
    data_type=melted_df['month_year'].dtype
    #print(data_type)
    find_type=melted_df['month_year'] = pd.to_datetime(melted_df['month_year'])
    #print(find_type.dtype)
    # Change the date format to 'dd/mm/yyyy'
    melted_df['month_year'] = melted_df['month_year'].dt.strftime('%d/%m/%Y')

    melted_df.to_csv(os.path.join(DIR_PATH,FOLDER_NAME, 'jodi_data.csv'), index=False)

def load_file(DIR_PATH,FOLDER_NAME,FILE_NAME):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="postgres-1.cxxjmzypafht.us-east-2.rds.amazonaws.com",
        database="postgres",
        user="postgres",
        password="Og912uD1JSrdiUwrolm8"
    )
    print("connected database")
    # Create a table in the database
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE jodi (
        country VARCHAR, month_year DATE, value FLOAT

        )
    """)
    print("Table created")
    # Import the CSV file into the table
    with open(os.path.join(DIR_PATH,FOLDER_NAME, FILE_NAME), 'r') as f:
        next(f)  # Skip the header row
        cur.copy_from(f, 'jodi', sep=',')

    # Commit the changes to the database
    conn.commit()

    # Close the connection to the database
    conn.close()
 
file_download(DIR_PATH, FOLDER_NAME)
process_file(DIR_PATH,FOLDER_NAME,FILE_NAME)
load_file(DIR_PATH,FOLDER_NAME, FILE_NAME)
