# -*- coding: utf-8 -*-
"""
바코드 웹 (시작페이지)
Created on Wed Feb  5 15:48:57 2020

@author: 20605019(정현배)
"""

import pandas as pd
import pymssql
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
        
        # @eunmi : 바코드 로그 가져오기 (.json)
        if (data=='barcode'):
            # 1. DB 주소 정보
            server = '10.224.1.86'
            # database = 'DNA'
            username = 'dna_svc'
            password = 'dna@Kefic0'
        
            # 2. DB 연결 및 Table 데이터 가져오기
            conn = pymssql.connect(server=server, user=username, password=password)
            stmt = "select * from df_MAT"
            df_MAT = pd.read_sql(stmt, conn)
            stmt = "select * from KMP_nodeInfo"
            df_nodes = pd.read_sql(stmt, conn)
            
            # 3. 데이터 가공 (df_MAT)
            links = pd.pivot_table(df_MAT, index=['source', 'target'], values=['value'], aggfunc=len)
            links.sort_values(by='value', ascending=False, inplace=True)
            links.reset_index(level=[0, 1], inplace=True)
        
            # 4. Json 문자열 변환
            sankey = {}
            sankey['nodes'] = json.loads(df_nodes.to_json(orient='records'))
            sankey['links'] = json.loads(links.to_json(orient='records'))
            sankey = json.dumps(sankey)
        else:        
            #@eunmi : 로컬 경로
            with open("D:/2. 분석 업무/BI_멕시코 바코드 분석/KBarCode-master/static/" + data + ".json") as f: 
                # @eunmi : Json 문자열 변환
                sankey = f.read()
                sankey = sankey.replace("\n", "").replace(" ", "")
        
        return sankey
    
    # 웹서버 구동
    app.run(host='127.0.0.1',port=8080,debug=True)
    
# 이 웹서버는 127.0.0.1 주소를 가지면 포트 80번에 동작하며, 에러를 자세히 표시한다. 
if __name__ == "__main__":
    main()





