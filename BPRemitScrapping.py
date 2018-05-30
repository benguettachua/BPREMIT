
#import the library used to query a website
import urllib.request as urllib2

#import the pandas dataframe for dataframe creation
import pandas as pd

#import sqlite to allow storing of data into sql database
import sqlite3 as lite

#import the Beautiful soup functions to parse the data returned from the website
from bs4 import BeautifulSoup

#to help convert http requests into json and stored into data table
import json
import requests

#helps to perform string manipulation
import re

#method to scrape BP Remits
def scrapeBPRemits(right_table):    
    #iterating through the html tags to assigned the values into the respective columns
    for row in right_table.findAll("tr"):

        #extract the postID from the tr tag to be used for 'see revisions' later on
        postID.append(row['data-id'])

        #only looks at the span tags as data are within the tags
        cells = row.findAll('span')
        if len(cells)==19:
            #messageID uses different method to remove the view revisions tab
            messageID.append(re.sub('\s+',' ',cells[0].find(text=True)).strip())

            #other fields uses the same method to get text as it automatically removes space, /t and /n
            ummType.append(cells[1].get_text(" ", strip=True))
            publicationDateTime.append(cells[2].get_text(" ", strip=True))
            eventType.append(cells[3].get_text(" ", strip=True))
            eventStart.append(cells[4].get_text(" ", strip=True))
            eventStop.append(cells[5].get_text(" ", strip=True))
            eventStatus.append(cells[6].get_text(" ", strip=True))
            affectedAssetOrUnit.append(cells[7].get_text(" ", strip=True))
            biddingZoneOrBalancingZone.append(cells[8].get_text(" ", strip=True))
            availabilityType.append(cells[9].get_text(" ", strip=True))
            unavailableCapacity.append(cells[10].get_text(" ", strip=True))
            availableCapacity.append(cells[11].get_text(" ", strip=True))
            installedCapacityOrTechnicalCapacity.append(cells[12].get_text(" ", strip=True))
            unitMeasure.append(cells[13].get_text(" ", strip=True))
            unavailabilityReason.append(cells[14].get_text(" ", strip=True))
            remarks.append(cells[15].get_text(" ", strip=True))
            fuelType.append(cells[16].get_text(" ", strip=True))
            marketParticipant.append(cells[17].get_text(" ", strip=True))
            messageIDNum.append(cells[18].get_text(" ", strip=True))


#method to scrape the revision data by making use of the post ID extracted from the html
def scrape_revisions(postID):
    
    #iterates through all the codes to identify if there are revisions
    for postId in postID:
        #initialise the parameters for post request
        payload = { 
            'action': 'ajaxRemitRevisionsGet',
            'post_id': postId,
            'nonce': '0186a6ee32'
        }
        
        #send the post request
        r = requests.post(
            url='https://remit.bp.com/wp/wp-admin/admin-ajax.php',
            data=payload,
            headers={
                'X-Requested-With': 'XMLHttpRequest'
            }
        )
        
        #converts the results into json by removing away the tags
        s = BeautifulSoup(r.text, "lxml").text
        d = json.loads(s)
        listOfData = d['response']['posts']
        
        #stores them into the respective lists to be converted into dataframe later on
        for row in listOfData:
            messageID.append(row['post_title'])
            ummType.append(row['ummType'])
            publicationDateTime.append(row['publicationDateTime'])
            eventType.append(row['eventType'])
            eventStart.append(row['eventStart'])
            eventStop.append(row['eventStop'])
            eventStatus.append(row['eventStatus'])
            affectedAssetOrUnit.append(row['merge_7'])
            biddingZoneOrBalancingZone.append(row['merge_8'])
            availabilityType.append(row['availabilityType'])
            unavailableCapacity.append(row['unavailableCapacity'])
            availableCapacity.append(row['availableCapacity'])
            installedCapacityOrTechnicalCapacity.append(row['merge_14'])
            unitMeasure.append(row['unitMeasure'])
            unavailabilityReason.append(row['unavailabilityReason'])
            remarks.append(row['remarks'])
            fuelType.append(row['fuelType'])
            marketParticipant.append(row['merge_21'])
            messageIDNum.append(row['messageIDNum'])
 

#method to create a dataframe using the list of data scraped from the website
def dataframeCreation():    
    #initialising the columns with the respective list created
    df['MESSAGE_ID']=messageID
    df['UMM_TYPE']=ummType
    df['PUBLICATION_DATE_AND_TIME']=publicationDateTime
    df['PUBLICATION_DATE_AND_TIME'] =pd.to_datetime(df['PUBLICATION_DATE_AND_TIME'])
    df['TYPE_OF_EVENT']=eventType
    df['EVENT_START']=eventStart
    df['EVENT_START'] = pd.to_datetime(df['EVENT_START'])
    df['EVENT_STOP']=eventStop
    df['EVENT_STOP'] = pd.to_datetime(df['EVENT_STOP'])
    df['EVENT_STATUS']=eventStatus
    df['AFFECTED_ASSET_OR_UNIT_OR_EIC']=affectedAssetOrUnit
    df['BIDDING_ZONE_OR_BALANCING_ZONE']=biddingZoneOrBalancingZone
    df['TYPE_OF_UNAVAILABILITY']=availabilityType
    df['UNAVAILABLE_CAPACITY']=unavailableCapacity
    df['UNAVAILABLE_CAPACITY'] = pd.to_numeric(df['UNAVAILABLE_CAPACITY'])
    df['AVAILABLE_CAPACITY']=availableCapacity
    df['AVAILABLE_CAPACITY'] = pd.to_numeric(df['AVAILABLE_CAPACITY'])
    df['INSTALLED_CAPACITY_OR_TECHNICAL_CAPACITY']=installedCapacityOrTechnicalCapacity
    df['INSTALLED_CAPACITY_OR_TECHNICAL_CAPACITY'] = pd.to_numeric(df['INSTALLED_CAPACITY_OR_TECHNICAL_CAPACITY'])
    df['UNIT_OF_MEASUREMENT']=unitMeasure
    df['REASON_FOR_THE_UNAVAILABILITY']=unavailabilityReason
    df['REMARKS']=remarks
    df['FUEL_TYPE']=fuelType
    df['MARKET_PARTICIPATION_OR_EIC']=marketParticipant
    df['MESSAGE_ID_NUM']=messageIDNum

    #doing replacement to replace empty, nan or '-' fields with null
    df.replace('', 'null', inplace = True)
    df.replace('-', 'null', inplace = True)
    df.replace('nan', 'null', inplace = True)
    df.replace('None', 'null', inplace = True)

#method to store data into SQLite3 using SQL database name and initializing the sql connection.
def databaseLoading():
    db_filename = r'BPREMIT'
    con = lite.connect(db_filename)

    #iterates 1 by 1 to check if row of data is duplicated within the database, if it is, skips and continue to the next line
    for i in range(len(df)):
        try:
        ## Convert to dataframe and write to sql database.
            df.iloc[i:i+1].to_sql('BPREMIT', con,
                    schema=None, if_exists='append', index=False, chunksize=None, dtype=None)
        except lite.IntegrityError: 
            print ('There are duplicates already in the database.')
            pass

    ## Close the SQL connection
    con.close()

#main method
if __name__ == '__main__':
    
    #specify the url
    page_to_extract = "https://remit.bp.com/"

    #try catch to handle situations when website or connection is down
    try:
        #Query the website and return the html to the variable 'page'
        page = urllib2.urlopen(page_to_extract)

    #catches the error
    except ValueError:
        print ("Website is unavailable/ There is no internet connection")

    #Parse the html in the 'page' variable, and store it in Beautiful Soup format
    soup = BeautifulSoup(page, "lxml")

    #identifies the body part of the html as the data are contained inside the tbody tags
    right_table=soup.find('table', {"class":'data data--body'})
    right_table = right_table.tbody
    
    #Generate lists for dataframe creation later on
    messageID=[]
    ummType=[]
    publicationDateTime=[]
    eventType=[]
    eventStart=[]
    eventStop=[]
    eventStatus=[]
    affectedAssetOrUnit=[]
    biddingZoneOrBalancingZone=[]
    availabilityType=[]
    unavailableCapacity=[]
    availableCapacity=[]
    installedCapacityOrTechnicalCapacity=[]
    unitMeasure=[]
    unavailabilityReason=[]
    remarks=[]
    fuelType=[]
    marketParticipant=[]
    messageIDNum=[]
    postID = []
    
    #initializes the dataframe
    df=pd.DataFrame(messageID,columns=['MESSAGE_ID'])
    
    #initialized the methods for scraping data and turning them into dataframes for loading into database.
    scrapeBPRemits(right_table)
    scrape_revisions(postID)
    dataframeCreation()
    databaseLoading()


