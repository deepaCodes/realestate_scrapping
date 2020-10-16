import requests

url = 'https://www.googleapis.com/customsearch/v1'

params = {'key': 'AIzaSyCfKDZksmFlOp1SmCOeuiTOUiDEjO7HAUs',
          'cx': '6425e4264a3bd1aed',
          'q': 'Le Moyne-Owen College',
          'startIndex': 1, 'count': 10
          }

payload = {}
headers = {}

response = requests.get(url, params=params)
result = response.json()

print(result)
