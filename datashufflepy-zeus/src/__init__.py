# -*- coding: utf-8 -*-
import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
# rootPath = os.path.split(curPath)[0]
sys.path.append(curPath)
# print(curPath)
if __name__ == '__main__':

    try:
        i=1/0
    except Exception as e:
        a,b,c=sys.exc_info()
        print(a)
        print(b)
        print(c)