REDFIN_HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,kn;q=0.8',
    'cache-control': 'no-cache',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',
}

OPEN_DATA_ARC_GIS_HEADERS = {
    'Origin': 'https://gisopendata-countyofriverside.opendata.arcgis.com',
    'Referer': 'https://gisopendata-countyofriverside.opendata.arcgis.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'
}

SELL_COMMISSION_PCT = 2.5
RED_FIN_BASE_URL = 'https://www.redfin.com'

PROPERTY_DETAILS = '{}/stingray/api/home/details/belowTheFold'.format(RED_FIN_BASE_URL)

OPEN_DATA_ARC_GIS_API = 'https://gis.countyofriverside.us/arcgis_public/rest/services/OpenData/ParcelBasic/MapServer/4/query'
# ARC_GIS_RETURN_FIELDS = 'APN,HOUSE_NO,STREET,SUFFIX,CITY,ZIP,ACRE,CAME_FROM,LAND,STRUCTURE,OWNER1_LAST_NAME,OWNER1_FIRST_NAME,OWNER1_MID_NAME,OWNER2_LAST_NAME,OWNER2_FIRST_NAME,OWNER2_MID_NAME,OWNER3_LAST_NAME,OWNER3_FIRST_NAME,OWNER3_MID_NAME'
ARC_GIS_RETURN_FIELDS = 'APN,MAIL_TO_NAME,MAIL_TO_STREET,MAIL_TO_CITY,MAIL_TO_ZIP,HOUSE_NO,STREET,SUFFIX,CITY,ZIP,OWNER1_LAST_NAME,OWNER1_FIRST_NAME,OWNER1_MID_NAME,OWNER2_LAST_NAME,OWNER2_FIRST_NAME,OWNER2_MID_NAME,OWNER3_LAST_NAME,OWNER3_FIRST_NAME,OWNER3_MID_NAME'

ARC_GIS_PARAM = {
    'f': 'json',
    'outFields': ARC_GIS_RETURN_FIELDS,
    'returnGeometry': 'false',
    'returnDistinctValues': 'true',
}
# ARC_GIS_WHERE_CLAUSE = "CITY='{}' AND ZIP={} AND HOUSE_NO={}"
ARC_GIS_WHERE_CLAUSE = "APN='{}'"

DOC_SEARCH_POST_API = 'https://webselfservice.riversideacr.com/Web/searchPost/DOCSEARCH2111S5'
DOC_SEARCH_GET_API = 'https://webselfservice.riversideacr.com/Web/searchResults/DOCSEARCH2111S5?page=1&_={}'

DOC_SEARCH_POST_API_PAYLOAD = 'field_DocumentNumberID=&field_RecordingDateID_DOT_StartDate=&field_RecordingDateID_DOT_EndDate=&field_BothNamesID=&field_ParcelID={}&field_selfservice_documentTypes-containsInput=Contains+Any&field_selfservice_documentTypes=&field_UseAdvancedSearch='
DOC_SEARCH_POST_HEADERS = {
    'Host': 'webselfservice.riversideacr.com',
    'Accept': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://webselfservice.riversideacr.com',
    'Referer': 'https://webselfservice.riversideacr.com/Web/search/DOCSEARCH2111S5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,kn;q=0.8',
}

DOC_SEARCH_GET_HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,kn;q=0.8',
    'Host': 'webselfservice.riversideacr.com',
    'Referer': 'https://webselfservice.riversideacr.com/Web/search/DOCSEARCH2111S5',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

TRUST_KEYS = [' trust', ' trustee', ' tte']
