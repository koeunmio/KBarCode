# -*- coding: utf-8 -*-
"""
바코드 웹 (시작페이지)
Created on Wed Feb  5 15:48:57 2020

@author: 20605019(정현배)
"""

import pandas as pd
import json
from flask import Flask, render_template

def main():
    # flask web 실행
    app = Flask(__name__)
     
    # /를 index() 함수로 연결
    @app.route("/")
    def index():
        return render_template('index.html')
    
    @app.route("/<page>")
    def templates(page):
        return render_template(page + '.html')

    @app.route("/json_data/<data>")
    def json_data(data):
        
        sankey = None
        
        with open("D:/KBarCode/static/" + data + ".json") as f:
            sankey = json.load(f)
        
        return json.dumps(sankey, indent=4)
    
    # 웹서버 구동
    app.run(host='127.0.0.1',port=8080,debug=True)
    
# 이 웹서버는 127.0.0.1 주소를 가지면 포트 80번에 동작하며, 에러를 자세히 표시한다. 
if __name__ == "__main__":
    main()





