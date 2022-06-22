from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.support.select import Select
import uuid
from os.path import exists
import os
import urllib
import boto3
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Functions TODO: test the help documentation for this code
# TODO: replace time.sleep with WebDriverWait
def load_page() -> webdriver.Chrome: 
    '''
    Function to open the Rightmove URL and accept cookies
    '''
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
    driver.get('https://www.rightmove.co.uk/house-prices.html')
    time.sleep(5)  # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH,"//*[@title='Allow all cookies']"))) #This does not seem to work with XPATH, and I can't find an ID of the element
    driver.find_element(by=By.XPATH, value=("//*[@title='Allow all cookies']")).click()

    return driver

def input_postcode(driver, postcode:str) -> webdriver.Chrome : 
    '''
    Function to input the post code into the search bar and search for results
    '''
    driver.find_element(by=By.XPATH, value=("//*[@id='searchLocation']")).send_keys(postcode)
    driver.find_element(by=By.XPATH, value=("//*[@value='List View']")).click()

    return driver


def update_filters(driver, radius:str) ->webdriver.Chrome:
    '''
    Function to select the radius of the search restuls via a drop down filter

    '''
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID,"content")))
    select_element = driver.find_element(by=By.XPATH, value=("//*[@id='content']/div[2]/div[2]/div[4]/div[1]/div[3]/div[1]/div[2]/select"))
    select_object = Select(select_element)
    select_object.select_by_visible_text(radius)
    time.sleep(5) #TODO: Replace this line

    return driver

def extract_data(driver): #-> tuple[webdriver.Chrome, dict]: removing this type hint as it seems to thrown an error in docker image?
    '''
    Function to extract and save the data of the result search and
    save them to a file locally
    '''
    #Creation of local folders
    folder_exists = exists('raw_data')
    if folder_exists == False:
        os.mkdir('raw_data')
    
    property_container = driver.find_element(by=By.XPATH, value=("//*[@class='results ']"))
    property_list = property_container.find_elements(by=By.XPATH, value=(".//*[@class='propertyCard-content']"))
    link_dictionary ={}

    property_list = property_list[0:3] #Remove this line to scrape all data
    for property in property_list:
        #For each property, generate a UUID, and extract the URL, property type, beds, price, sale date
        # Inside a try loop, as there are some records with missing data which we want to skip
        try:
            id = uuid.uuid4()

            property_url = property.find_element(by=By.XPATH, value=(".//a[@class='title clickable']"))
            url = property_url.get_attribute('href')

            #The URL contains an ID for the property and for the most recent sale.
            # Extract thse two pieces of data to generate our own unique reference ('ref')
            url_split = url.split('england-')
            ref = url_split[1]

            #stop here if we already have saved this data and return to start of for loop
            folder_exists = exists('raw_data/' + ref)
            if folder_exists == True:
                continue
            
            #This link dictionary will be used later to extract the photos for each entry
            link_dictionary[ref] = url

            #Etract the Type, number of beds, price and sold date
            property_type = property.find_element(by=By.XPATH, value=(".//*[@class='propertyType']"))
            type = property_type.text
            subtitle = property.find_element(by=By.XPATH, value=(".//*[@class='subTitle bedrooms']"))
            beds = subtitle.text
            property_price = property.find_element(by=By.XPATH, value=(".//*[@class='price']"))
            price = property_price.text
            property_sold_date = property.find_element(by=By.XPATH, value=(".//*[@class='date-sold']"))
            sold_date = property_sold_date.text

            #Store data as a dictionary and write this to a local file
            property_dictionary = {"id": id, "ref": ref, "link":url,"type": type, "beds":beds, "sale_price":price, "date_sold":sold_date}
            os.mkdir(f'raw_data/{ref}')
            with open(f'raw_data/{ref}/data.json','w') as data:
                data.write(str(property_dictionary))
            
            # convert to a dataframe so that we can upload to PostgreSQL DB
            df = pd.DataFrame([property_dictionary]) 
            df = df.set_index('ref') 

            #Connect to DB
            load_dotenv()
            DATABASE_TYPE = 'postgresql'
            DBAPI = 'psycopg2'
            ENDPOINT = 'aicoredb.cwe7sdipxebo.us-east-1.rds.amazonaws.com' 
            USER =os.getenv("DB_USER")
            PASSWORD = os.getenv("DB_PASSWORD")
            PORT = 5432
            DATABASE = 'postgres'
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
            try:
             df.to_sql('web-scraper-project', engine, if_exists='append') #I wrapped this in an try/except because if the record already exists it comes back with an error. Is this an OK way of handling that?
            except Exception as e:
                logger.error('exception in writing to DB: '+ str(e))  
                pass
            
            
        except Exception as e:
            logger.error('exception in extracting data function: '+ str(e))  
            pass

    return driver, link_dictionary

def extract_pictures(link_dictionary:dict) -> webdriver.Chrome:
    '''
    Function to extract the pictures from the results and save these
    '''
    #For all the URLs, navigate to these, and extract the photots
    for key in link_dictionary:
        try:
            driver.get(link_dictionary[key])
            time.sleep(3)#TODO Update this line


            image_container = driver.find_element(by=By.XPATH, value=("//div[@class='yyidGoi1pN3HEaahsw3bi']"))
            image_thumbnails = image_container.find_elements(by=By.XPATH, value=(".//img[@data-testid='image']"))
            number_of_images = len(image_thumbnails)

            x = 0
            for i in image_thumbnails:
                x = x + 1
                image_url = i.get_attribute('src')
                f = urllib.request.urlopen(image_url)
                myfile = f.read()
                writeFileObj = open(f'raw_data/{key}/{x}.jpeg','wb')
                writeFileObj.write(myfile)
                writeFileObj.close()

        except Exception as e:
            logger.error('exception in extract_pictures: '+ str(e))    
            pass

    return driver

def list_of_pages(driver) -> list:
    page_number_container = driver.find_element(by=By.XPATH, value=("//select[@name='currentPage']"))
    page_numbers = page_number_container.find_elements(by=By.XPATH, value=(".//option"))
    number_of_pages=len(page_numbers)
    pages=[]

    for i in range(number_of_pages):
        page = str(i + 1)
        url=('https://www.rightmove.co.uk/house-prices/hg3-4la.html?radius=1.0&page='+ page)
        pages.append(url)

    return pages


def upload_files_to_s3() -> None:

    session = boto3.Session(
        aws_access_key_id=os.environ.get('aws_access_key_id'),
        aws_secret_access_key=os.environ.get('aws_secret_access_key')
    )

    s3 = session.resource('s3')
    bucket = s3.Bucket('aircore-s3-bucket')

    # check to see whether parent folders exist, and if not, create them
    key = 'web-scraper-project'
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        pass
    else:
        bucket.put_object(Key= (key +'/'), Body='')

    key2 = 'web-scraper-project/raw_data'
    objs2 = list(bucket.objects.filter(Prefix=key2))
    if len(objs2) > 0 and objs2[0].key == key:
        pass
    else:
        bucket.put_object(Key= (key2 + '/'), Body='')

    # Now, to check to see whether the raw data has already been uploaded
    # and if not upload it

    directory = 'raw_data'
    for folder in os.listdir(directory):
        subfolder = os.path.join(directory, folder)

        key = 'web-scraper-project/' + subfolder
        objs = list(bucket.objects.filter(Prefix=key))
        if len(objs) > 0 and objs[0].key == key:
            pass
        else:
            bucket.put_object(Key= (key +'/'), Body='')
            
            key = 'web-scraper-project/' + subfolder

            s3_client = boto3.client('s3', 
                aws_access_key_id=os.environ.get('aws_access_key_id'),
                aws_secret_access_key=os.environ.get('aws_secret_access_key'))

            for file in os.listdir(subfolder):
                local_file_path = os.path.join(subfolder, file)
                s3_client.upload_file(local_file_path,'aircore-s3-bucket',(key +'/' + file))

    return None



if __name__ == "__main__":

    postcode = "HG3 4LA"
    radius = "Within 1 mile"

    #Load rightmove and accept cookies
    driver = load_page()

    # #Input postcode and click on "search" to get a list of results
    input_postcode(driver,postcode)

    # #Filter therse results
    update_filters(driver,radius)

    # #Extract data and pictures and save them locally and in our PostgreSQL DB
    pages = list_of_pages(driver)
    pages = pages[0:1] #Line inserted to throttle results for testing. Remove
    for page in pages:
        driver.get(page)
        time.sleep(6) #TODO update this line
        link_dictionary = extract_data(driver)[1]
        extract_pictures(link_dictionary)

    #Save data to AWS S3 bucket
    upload_files_to_s3()
    
