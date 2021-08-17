import requests

url = 'https://storage.cloud.google.com/sushant-julo-test-blucket/images/PNG_transparency_demonstration_1.png'
filename = url.split('/')[-1]
r = requests.get(url, allow_redirects=True)
open(filename, 'wb').write(r.content)