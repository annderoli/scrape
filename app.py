import requests
import json
import pandas as pd

url = ''

response = requests.get(url)

data = json.loads(response.text)

df = pd.DataFrame(data['instruments'])

df