import re
from urllib.parse import urlsplit, parse_qs, quote

url = "https://gis.countyofriverside.us/arcgis_public/rest/services/OpenData/ParcelBasic/MapServer/4/query?f=json&where=CITY='Temecula' AND ZIP=92592 AND HOUSE_NO = 35790&outFields=APN,HOUSE_NO,STREET,SUFFIX,CITY,ZIP,ACRE,CAME_FROM,LAND,STRUCTURE,OWNER1_LAST_NAME,OWNER1_FIRST_NAME,OWNER1_MID_NAME,OWNER2_LAST_NAME,OWNER2_FIRST_NAME,OWNER2_MID_NAME,OWNER3_LAST_NAME,OWNER3_FIRST_NAME,OWNER3_MID_NAME&returnGeometry=false&returnDistinctValues=true"


def decode_params():
    query = urlsplit(url).query
    params = parse_qs(query)
    print(params)

    where = "CITY = 'TEMECULA' AND HOUSE_NO >= 35790 AND HOUSE_NO <= 35790 AND ZIP >= 92592 AND ZIP <= 92592"
    where = "CITY='Temecula' AND ZIP=92592 AND HOUSE_NO = 35790"
    print(quote(where))


def extract_number_from_string():
    address = '26444 Arboretum Way 2108'
    print(address)
    result = int(re.search(r'\d+', address)[0])
    print(result)

def match_address():
    address = '31228 Brush Creek Cir'

def main():
    extract_number_from_string()


if __name__ == '__main__':
    main()
