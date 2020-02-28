# -*- coding: utf-8 -*-
import base64
from urllib import request

import requests

response = requests.get(url="https://img.21jingji.com/uploadfile/cover/20190316/201903160125334641.jpg")
base64_data = base64.b64encode(response.content)
print(base64_data)
s = base64_data.decode()
print('data:image/jpeg;base64,%s'%s)
# print(response.content)

# response = request.urlopen(url="https://img.21jingji.com/uploadfile/cover/20190316/201903160125334641.jpg")
# print(response.read())

with open("image.jpg", "wb") as f:
    f.write(response.content)

with open("xxx.txt", "wb") as f:
    f.write(base64_data)

with open("yyy.txt", "w", encoding="utf-8") as f:
    f.write(s)
