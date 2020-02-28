import base64

from mysql_db import MyPymysqlPool, Config

config = Config()
conn = MyPymysqlPool()
imges = conn.getAll('''select * from spi_captcha where TYPE_ = 'wechat2' and LENGTH(VALUE_) < 1;''')
for imge in imges:
    with open(r'./verify.png', 'wb') as fp:
        fp.write(base64.b64decode(imge.get('IMAGE_')))
    from PIL import Image
    image = Image.open(r'./verify.png')
    image.show()
    verify = input('请输入: ')
    id = imge.get('ID_')
    conn.update(f'''update spi_captcha set VALUE_ = '{verify}' where ID_ = '{id}' ;''')



