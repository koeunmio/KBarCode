# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import pymssql
import sqlalchemy
import urllib.parse

''' ---------------------------------------------------------------------------
1. 데이터 수집 :
    (1) 바코드 로그 테이블 : csv_w900
    (2) FCode 마스터 테이블 : KMP_FCode
'''
# (1) 바코드 로그 테이블 
server = '10.224.1.86'
database = 'DNA'
username = 'dna_svc'
password = 'dna@Kefic0'

conn = pymssql.connect(server=server, user=username,password=password)
stmt = "select * from csv_w900"

df_w900 = pd.read_sql(stmt, conn)

# (2) FCode 마스터 테이블
#@todo : FCode Table 연동 (Table 별도 생성 : KMP_FCode)
# Table 열 : FCode , FCode명
ex_list1 = ['0109', '0151', '0201', '0211', '0252', '0253', '0302', '0303', '0305', '0306', '0351', '0352', '0361', '0404', '0602', '0605', '0606', '0607', '0608', '0609', '0610', '0611', '0613', '0615', '0901', '0902']
ex_list2 = ['물류이동 입고', '완제품 창고 입고 (Rack)', '물류이동 출고 (플랜트간 출고 포함)', '물류이동출고(W/FIFO)', '수출 피킹', '고객 납품', '이적작업 (중포장->중포장)', '라벨복제', '라벨재발행 (SAP 전송)', '신규발행 (개별)', '팔레트 라벨 발행', '패킹라벨 조회', '고객라벨 매핑', '정기 재고실사', '자재이동 (보류 -> 보류)', '랙 이동', '창고 이동', '자재이동 (가용 -> 보류)', '자재이동 (보류 -> 가용)', '자재이동 (가용 -> 품질)', '품질검사 의뢰', '품질검사 확정', '품질검사 NG', '파렛트이동', '재고조회 - 품목번호로 조회', '재고조회 - RACK 번호로 조회']
df_FCode = pd.DataFrame(ex_list2, ex_list1)
df_FCode.reset_index(drop=False, inplace=True)
df_FCode.columns = ['FCode', 'FCode명']

''' ---------------------------------------------------------------------------
2. 데이터 필터링 :
    (1) "완제품 바코드시스템 박스라벨" 인 경우
    (2) "키"가 유효한 경우 ("키" = 자재번호+제조일+박스번호+사용위치)
    (3) "사용메뉴" 중 분석 불필요한 경우 (예 : 자재/랙 조회)
    (4) "제조일" 중 분석 불필요한 경우 (예 : 구축 안정화 전 데이터 로그)
    (5) 필요한 컬럼만 추출
'''
df_raw2 = df_w900.copy()
# (1) 필터링1 : 완제품 박스라벨 (M으로 시작) 인 경우 (# 166,131)
df_raw2['BoxType'] = df_raw2['Box_No_'].str[0:1]

df_raw2 = df_raw2.loc[ (df_raw2['BoxType'] == 'M') & 
                     (((df_raw2['Login_Site'] == 'M1') & (df_raw2['GR_Site'] == 'F1')) |
                      ((df_raw2['Login_Site'].isin(['F0', 'F1', 'FA'])))), :]  
                     
# (2) 필터링2 : "키"가 유효한 경우  (# 165,729 )
#    - 유효성 : 키 길이가 29인 경우
#    - 자재번호 : 10자리 
#    - 제조일   : 8자리
#    - 박스번호 : 6자리
#    - 사용위치 : 2자리
df_raw2['key'] = df_raw2['Material'].astype(str) + "|" + df_raw2['Manufacture_Date'].astype(str) + "|" + df_raw2['Box_No_'] + "|" + df_raw2['Login_Site']
df_raw2['key2'] = df_raw2['Material'].astype(str) + "|" + df_raw2['Manufacture_Date'].astype(str) + "|" + df_raw2['Box_No_']
df_raw2['key_len'] = df_raw2['key'].apply(lambda x : len(str(x))) 

#@todo : ADM 요청사항 (유효하지 않은 키에 대한 로그 개선 필요)
df_raw2['key_issue'] = ''
df_raw2.loc[ (df_raw2['key_len'] != 29), 'key_issue'] = '필수입력제어(조회메뉴 등)'

df_raw2 = df_raw2.loc[df_raw2['key_len'] == 29, : ]  

# (3) 필터링3 : 분석 불필요한 "사용메뉴" 필터링 (165,729건)
#    - FCode 타입 정리 : int -> object 형태로 정리
df_raw2['FCode'] = df_raw2['FCode'].astype(int)
df_raw2['FCode'] = "0" + df_raw2['FCode'].astype(str)
df_raw2 = df_raw2.loc[~(df_raw2['FCode'].isin(['0901', '0902'])), : ]
df_raw2 = pd.merge(df_raw2, df_FCode, on='FCode', how='left') # FCode Master Table 연동

# (4) 필터링4 : 분석 불필요한 "로그제조일" 필터링 (105,550건)
df_raw2 = df_raw2.loc[ df_raw2['Manufacture_Date'].astype(int) >= 20191202, : ]

# (5) 분석에 필요한 열만 추출 (105,550)
df_raw2 = df_raw2[['key', 'key2', 'Login_Site', 'FCode', 'FCode명', 'Creation_date', 'Time', 'Qty', 'WMS_Doc_', 'GR_Site',
                   'Material', 'Manufacture_Date', 'Box_No_', 'Created_by', 'key_len', 'BoxType', 'key_issue']]
df_raw2['Manufacture_Date'] = df_raw2['Manufacture_Date'].astype(int)
df_raw2['Time'] = df_raw2['Time'].astype(int)
df_raw2.sort_values(ascending=True, inplace=True, by=['key2', 'Creation_date', 'Time'])
df_raw2.reset_index(inplace=True, drop=True)

''' ---------------------------------------------------------------------------
3. 데이터 가공 (열 추가)
    (1) 이전처리한 FCode 찾기
    (2) 입고형태 : 정상입고 or 실사입고
    (3) 라벨형태 : 신규라벨 or 이적라벨 or 기존라벨
'''
df_EDA2 = df_raw2.copy()

# (1)-1. 이전 FCode 찾기 
dft_index = df_EDA2.copy()

dft_index.reset_index(inplace=True)
dft_index['index'] = dft_index['index'] + 1
dft_index.set_index('index', inplace=True)

# (1)-2. 기존 데이터프레임에 "이전FCode" 열 추가
# 조인 기준 : 자재번호+제조일+박스번호
df_EDA2 = pd.merge(df_EDA2, dft_index, left_index=True, right_index=True, how='left', suffixes=('', '_before'))
df_EDA2.loc[ (df_EDA2['key2'] != df_EDA2['key2_before']) , 'FCode_before'] = 'None'
df_EDA2.loc[ (df_EDA2['key2'] != df_EDA2['key2_before']) , 'Login_Site_before'] = 'None'

# (1)-3. 데이터프레임 열 순서 배치
df_EDA2 = df_EDA2[ ['key', 'key2', 'Login_Site', 'FCode', 'GR_Site', 'Login_Site_before', 'FCode_before', 
                    'Creation_date', 'Time', 'Qty', 'WMS_Doc_', 'Material', 
                    'Manufacture_Date', 'Box_No_', 'Created_by', 'key_issue', 'FCode명']]
df_EDA2.reset_index(drop=True, inplace=True)
df_EDA2.sort_values(ascending=True, inplace=True, by=['key2', 'Creation_date', 'Time'])

'''
3. 데이터 가공
    (2) 입고형태 :
        - 정상입고 : FCode (품질 : F0/0610) (영업 : F1/0151) (외주 : FA/0109)
        - 실사입고 : ~(정상입고) & (FCode : 0404)
        - 이적입고 : FCode == 0302
        - 미입고
        - 상품입고 : #@todo
'''
# 정상입고 처리된 key 값 추출
key_GR = pd.DataFrame(df_EDA2.loc[ ((df_EDA2['Login_Site'] == 'F0') & (df_EDA2['FCode'] == '0610')) |
                                  ((df_EDA2['Login_Site'] == 'F1') & (df_EDA2['FCode'] == '0151')) |
                                  ((df_EDA2['Login_Site'] == 'FA') & (df_EDA2['FCode'] == '0109')), 'key'])
key_GR = key_GR[key_GR.duplicated(['key'], keep='first')==False] # 35,731

# "실사입고" 처리된 key 값 추출
key_PI = pd.DataFrame(df_EDA2.loc[ ((df_EDA2['Login_Site'] == 'F0') & (df_EDA2['FCode'] == '0404')) |
                                  ((df_EDA2['Login_Site'] == 'F1') & (df_EDA2['FCode'] == '0404')) |
                                  ((df_EDA2['Login_Site'] == 'FA') & (df_EDA2['FCode'] == '0404')), 'key'])
key_PI = key_PI[key_PI.duplicated(['key'], keep='first')==False]  #3,778

# "이적입고" 처리된 key 값 추출
key_Change = pd.DataFrame(df_EDA2.loc[ ((df_EDA2['Login_Site'] == 'F0') & (df_EDA2['FCode'] == '0302')) |
                                  ((df_EDA2['Login_Site'] == 'F1') & (df_EDA2['FCode'] == '0302')) |
                                  ((df_EDA2['Login_Site'] == 'FA') & (df_EDA2['FCode'] == '0302')), 'key'])
key_Change = key_Change[key_Change.duplicated(['key'], keep='first')==False]  # 18

# key별 정상입고 or 실사입고 구분 (#@todo : 주석 수정)
df_EDA2['입고형태'] = None
df_EDA2.loc[df_EDA2['key'].isin(key_GR['key']) == True, '입고형태'] = '정상입고'

df_EDA2.loc[(df_EDA2['입고형태'].isnull()) &
            (df_EDA2['key'].isin(key_PI['key']) == True), '입고형태'] = '실사입고'
            
df_EDA2.loc[(df_EDA2['입고형태'].isnull()) &
            (df_EDA2['key'].isin(key_Change['key']) == True), '입고형태'] = '이적입고'
df_EDA2.loc[df_EDA2['입고형태'].isnull(), '입고형태'] = '미입고'
            

'''
3. 데이터 가공
    (3) 라벨형태 :
        - 신규라벨 : '0306' key  &  '0306'->'0404' key
        - 이적라벨 : '0302' key  &  '0302'->'0404' key
        - 기존라벨 : ~(신규라벨&이적라벨)
'''
df_EDA2['라벨형태'] = None

# 신규발행한 라벨 key 추출 
key_NewLabel = df_EDA2[df_EDA2.duplicated(['key'], keep='first')==False].loc[(df_EDA2['FCode']=='0306'), 'key']
df_EDA2.loc[df_EDA2.index.isin(key_NewLabel.index), '라벨형태'] = '신규라벨'

# 신규발행한 라벨 key 중 0404로 입고잡은 것
key_NewLabel_0404 = df_EDA2[(df_EDA2['key'].isin(key_NewLabel) == True) & (df_EDA2['FCode'] == '0404')]
key_NewLabel_0404 = key_NewLabel_0404[key_NewLabel_0404.duplicated(['key'], keep='first')==False]
df_EDA2.loc[df_EDA2.index.isin(key_NewLabel_0404.index), '라벨형태'] = '실사_신규라벨'

# 이적한 라벨 key 추출
key_changeLabel = df_EDA2[df_EDA2.duplicated(['key'], keep='first')==False].loc[(df_EDA2['FCode']=='0302'), 'key']
df_EDA2.loc[df_EDA2.index.isin(key_changeLabel.index), '라벨형태'] = '이적라벨'

# 이적한 라벨 key 중 0404로 입고잡은 것
key_changeLabel_0404 = df_EDA2[(df_EDA2['key'].isin(key_changeLabel) == True) & (df_EDA2['FCode'] == '0404')]
key_changeLabel_0404 = key_changeLabel_0404[key_changeLabel_0404.duplicated(['key'], keep='first')==False]
df_EDA2.loc[df_EDA2.index.isin(key_changeLabel_0404.index), '라벨형태'] = '실사_이적라벨'

# 기존 라벨
#@todo : 품질/검사확정 key --> 영업/key : 기존라벨
#@todo : 영업/창고출고 key --> 외주/key : 기존라벨
df_EDA2.loc[df_EDA2['라벨형태'].isnull(), '라벨형태'] = '기존라벨'

''' ---------------------------------------------------------------------------
4. 데이터 분석
    (1) 로그 처리결과 :
        - "박스종료"
            (1) 이적된 박스, '라벨형태(이적라벨)' & FCode_before.notnull()
        - 실사처리 세분화 (FCode 0404)
            (1) "실사입고" : 정상입고 메뉴로 처리안하고, 실사메뉴로 입고처리
            (2) "이적입고" : 이적된박스를 실사메뉴로 입고처리
            (3) "신규입고" : 신규라벨 발행 후 실사메뉴로 입고처리
            (4) "재실사 (랙실사 or 가용처리)" : 실사메뉴 중복 처리
        - 메뉴 중복사용 사유 구분
            (1) 중복 비허용 메뉴, "연속 처리로그 의심"됨 : 연속 중복처리 불가한데, 로그에 중복 찍힘
                - 제외 FCode : '0404', '0361', '0605', '0305'는 제외(제외 사유: 중복허용 메뉴)
            (2) 중복허용 메뉴 : 메뉴사용이 "중복처리"된 경우 
                - 허용 FCode : '0404', '0361', '0605', '0305' 중 '0361' 점검
                - 메뉴 0361  : 고객라벨매핑 화면, 중복데이터 중 최신 결과만 유효
        - "정상처리" : 위 조건에 해당 무
'''
df_data2 = df_EDA2.copy()
df_data2['처리결과'] = None

# (1) 무효된 박스 : 박스종료
df_data2.loc[ (df_data2['처리결과'].isnull()) &
             (df_data2['FCode'] == '0302') &
             (df_data2['라벨형태'] != '이적라벨'), '처리결과' ] = '박스종료'
             
# (2) 실사처리 구분 : 실사/이적/신규입고 or 재실사(랙실사 or 가용처리)   
# 중복 처리된 key의 index 구하기 
df_re0404 = df_data2.loc[(df_data2['FCode']=='0404'), ['key']]
df_re0404.loc[df_re0404.duplicated(['key'], keep='first')==True, '중복여부'] = '중복'  # 최초 처리가 유효
df_re0404.loc[df_re0404.duplicated(['key'], keep='first')==False, '중복여부'] = '유효'  # 최초 처리가 유효

# 실사처리 구분
df_data2.loc[ (df_data2['처리결과'].isnull()) &
             (df_data2['FCode'] == '0404') &
             (df_data2.index.isin(df_re0404[(df_re0404['중복여부'] == '유효')].index)) & # 첫 0404 처리 (중복발생 전)
             (df_data2['라벨형태'] == '실사_신규라벨'), '처리결과'] = '신규입고'          # 신규라벨 발행 및 실사입고 (정상입고 미처리)
df_data2.loc[ (df_data2['처리결과'].isnull()) &
             (df_data2['FCode'] == '0404') &
             (df_data2.index.isin(df_re0404[(df_re0404['중복여부'] == '유효')].index)) & # 첫 0404 처리 (중복발생 전)
             (df_data2['라벨형태'] == '실사_이적라벨'), '처리결과'] = '이적입고'          # 박스 이적했으나(라벨 신규발행), 정상입고 형태로 처리하지 않은 경우
df_data2.loc[(df_data2['처리결과'].isnull()) &
             (df_data2['FCode'] == '0404') &
             (df_data2.index.isin(df_re0404[(df_re0404['중복여부'] == '유효')].index)) & # 첫 0404 처리 (중복발생 전)
             (df_data2['입고형태'] == '실사입고' ), '처리결과'] = '실사입고'              # 기존 라벨에 대해 정상입고 처리하지 않은 경우
df_data2.loc[ (df_data2['처리결과'].isnull()) &
             (df_data2['FCode'] == '0404') , '처리결과'] = '재실사(랙실사 or 가용처리)'

# (3)중복처리 점검 : 정상처리 or 중복처리 or 처리의심 구분 
# (3)-1. 중복처리 의심 : 중복 처리가 될 수 없는 메뉴인데, 로그상에 찍힌 데이터들 
df_data2.loc[ (df_data2['Login_Site'] == df_data2['Login_Site_before']) & 
             (df_data2['FCode'] == df_data2['FCode_before']) & 
             ~(df_data2['FCode'].isin(['0404', '0361', '0605', '0305']))  , '처리결과' ] = '연속처리_의심' 
# (3)-2. 고객라벨매핑
# 중복 처리된 key의 index 구하기 
df_re0361 = df_data2.loc[(df_data2['FCode']=='0361'), ['key']]
df_re0361.loc[df_re0361.duplicated(['key'], keep='last')==True, '중복여부'] = '중복'  # 마지막 처리가 유효
df_re0361.loc[df_re0361.duplicated(['key'], keep='last')==False, '중복여부'] = '유효'  # 마지막 처리가 유효

df_data2.loc[ (df_data2['처리결과'].isnull()) &
             (df_data2['FCode'] == '0361') &
             (df_data2.index.isin(df_re0361[(df_re0361['중복여부'] == '중복')].index)), '처리결과'] = '중복처리'
df_data2.loc[ (df_data2['처리결과'].isnull()) &
             (df_data2['FCode'] == '0361') &
             (df_data2.index.isin(df_re0361[(df_re0361['중복여부'] == '유효')].index)), '처리결과'] = '정상처리'

# (3)-3. 기타 중복처리된 데이터 
df_data2.loc[ (df_data2['처리결과'].isnull()) &
             (df_data2.duplicated(['key', 'FCode'], keep='first') == True ), '처리결과' ] = '중복처리' 
# (4) 정상처리 데이터
df_data2.loc[ (df_data2['처리결과'].isnull()), '처리결과'] = '정상처리'

''' ---------------------------------------------------------------------------
4. 데이터 분석 :
    (2) source/target 정의 (창고이동 명확화)
    - 품질 : 창고입고, 창고운영
    - 영업 : 창고입고, 창고운영, 창고출고(이동중재고)
    - 외주 : 창고입고, 창고운영, 고객납품
'''
#2. Sankey table
df_MAT = df_data2.copy()

df_MAT['source'] = ''
df_MAT['target'] = ''
df_MAT['value'] = df_MAT['Qty']

'''
[source, target] 정의 :
    품질창고 [source, target]:
        - 입고처리 - [ , ] : 
            (1) FCode(0610) 
            (2) FCode(0404) & 처리결과(실사입고)
'''
df_MAT.loc[ df_MAT['FCode'] == '0610' , ['source', 'target'] ] = ['★생산창고★', '★품질창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0306') & 
            (df_MAT['Created_by'].str.contains('QCT')), ['source', 'target'] ] = ['신규라벨', '★품질창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('QCT')) &
            (df_MAT['처리결과'] == '신규입고'), ['source', 'target'] ] = ['품질_신규입고', '★품질창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('QCT')) &
            (df_MAT['처리결과'] == '실사입고'), ['source', 'target'] ] = ['품질_실사입고', '★품질창고★']

'''
[source, target] 정의 :
    품질창고 [source, target]:
        - 창고운영 [source, target]:
'''
df_MAT.loc[ df_MAT['FCode'] == '0611' , ['source', 'target'] ] = ['★품질창고★', '품질검사OK']

df_MAT.loc[ (df_MAT['FCode'] == '0302') & 
            (df_MAT['FCode_before'] == 'None') & 
            (df_MAT['Created_by'].str.contains('QCT')) , ['source', 'target'] ] = ['품질창고_기타', '품질_박스이적']
df_MAT.loc[ (df_MAT['FCode'] == '0609') &
            (df_MAT['Created_by'].str.contains('QCT')), ['source', 'target'] ] = ['품질창고_기타', '품질_상태변경']
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('QCT')) &
            (df_MAT['처리결과'] == '재실사(랙실사 or 가용처리)'), ['source', 'target'] ] = ['품질창고_기타', '품질_재실사']

'''
[source, target] 정의 :
    완제품창고 [source, target]:
        - 입고처리 - [ , ] : 
            (1) FCode(0151) 
            (2) FCode(0404) & 처리결과(실사입고)
'''
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('SCT')) &
            (df_MAT['처리결과'] == '신규입고'), ['source', 'target'] ] = ['영업_신규입고', '★완제품창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('SCT')) &
            (df_MAT['처리결과'] == '실사입고'), ['source', 'target'] ] = ['영업_실사입고', '★완제품창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('SCT')) &
            (df_MAT['처리결과'] == '이적입고'), ['source', 'target'] ] = ['영업_이적라벨입고', '★완제품창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0306') & 
            (df_MAT['Created_by'].str.contains('SCT')), ['source', 'target'] ] = ['신규라벨', '★완제품창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0151'), ['source', 'target'] ] = ['품질검사OK', '★완제품창고★']
df_MAT.loc[ (df_MAT['Login_Site'] == 'M1') & 
            (df_MAT['FCode'] == '0601'), ['source', 'target'] ] = ['상품입고', '★완제품창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0109') & 
            (df_MAT['Created_by'].str.contains('SCT')) , ['source', 'target'] ] = ['완창고_출고', '완제품창고_기타']

'''
[source, target] 정의 :
    완제품창고 [source, target]:
        - 창고운영 [source, target]:
'''
df_MAT.loc[ (df_MAT['FCode'] == '0609') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['완제품창고_기타', '품질재고']
df_MAT.loc[ (df_MAT['FCode'] == '0605') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['완제품창고_기타', '랙이동']
df_MAT.loc[ (df_MAT['FCode'] == '0606') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['완제품창고_기타', '창고이동']
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('SCT')) &
            (df_MAT['처리결과'] == '재실사(랙실사 or 가용처리)'), ['source', 'target'] ] = ['완제품창고_기타', '영업_재실사']
df_MAT.loc[ (df_MAT['FCode'] == '0302') & 
            (df_MAT['FCode_before'] == 'None') & 
            (df_MAT['Created_by'].str.contains('SCT')) , ['source', 'target'] ] = ['완제품창고_기타', '영업_박스이적']
df_MAT.loc[ (df_MAT['FCode'] == '0302') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['완제품창고_기타', '이적']
df_MAT.loc[ (df_MAT['FCode'] == '0303') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['완제품창고_기타', '완제품_라벨복제']
df_MAT.loc[ (df_MAT['FCode'] == '0305') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['완제품창고_기타', '라벨재발행']
df_MAT.loc[ (df_MAT['FCode'] == '0351') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['★완제품창고★', '팔레트라벨']
df_MAT.loc[ (df_MAT['FCode'] == '0361') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['팔레트라벨', '고객라벨맵핑']
df_MAT.loc[ (df_MAT['FCode'] == '0253') & 
           df_MAT['Created_by'].str.contains('SCT') , ['source', 'target'] ] = ['고객라벨맵핑', '고객납품']
df_MAT.loc[ (df_MAT['FCode'] == '0302') & 
            (df_MAT['FCode_before'] != 'None') & 
            (df_MAT['Created_by'].str.contains('SCT')) , ['source', 'target'] ] = ['완제품창고_기타', '박스종료']
df_MAT.loc[ df_MAT['FCode'] == '0615' , ['source', 'target'] ] = ['완제품창고_기타', '파렛트이동']

'''
[source, target] 정의 :
    완제품창고 [source, target]:
        - 출고처리 [source, target]:
'''
df_MAT.loc[ df_MAT['FCode'] == '0252' , ['source', 'target'] ] = ['팔레트라벨', '수출피킹']
df_MAT.loc[ df_MAT['FCode'] == '0211' , ['source', 'target'] ] = ['팔레트라벨', '외주창고_출고']

'''
[source, target] 정의 :
    외주창고 [source, target]:
        - 입고처리 - [ , ] : 
            (1) FCode(0109) 
            (2) FCode(0404) & 처리결과(실사입고)
'''
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('GLV')) &
            (df_MAT['처리결과'] == '신규입고'), ['source', 'target'] ] = ['외주_신규입고', '★외주창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('GLV')) &
            (df_MAT['처리결과'] == '실사입고'), ['source', 'target'] ] = ['외주_실사입고', '★외주창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0306') & 
            (df_MAT['Created_by'].str.contains('GLV')), ['source', 'target'] ] = ['신규라벨', '★외주창고★']
df_MAT.loc[ (df_MAT['FCode'] == '0109') & 
            (df_MAT['Created_by'].str.contains('GLV')) , ['source', 'target'] ] = ['외주창고_출고', '★외주창고★']

'''
[source, target] 정의 :
    외주창고 [source, target]:
        - 창고운영 [source, target]:
'''
df_MAT.loc[ (df_MAT['FCode'] == '0361') & 
           df_MAT['Created_by'].str.contains('GLV') , ['source', 'target'] ] = ['★외주창고★', '고객라벨맵핑']
df_MAT.loc[ (df_MAT['FCode'] == '0253') & 
           df_MAT['Created_by'].str.contains('GLV') , ['source', 'target'] ] = ['고객라벨맵핑', '고객납품']
df_MAT.loc[ (df_MAT['FCode'] == '0303') & 
           df_MAT['Created_by'].str.contains('GLV') , ['source', 'target'] ] = ['외주창고_기타', '외주_라벨복제']
df_MAT.loc[ (df_MAT['FCode'] == '0404') &
            (df_MAT['Created_by'].str.contains('GLV')) &
            (df_MAT['처리결과'] == '재실사(랙실사 or 가용처리)'), ['source', 'target'] ] = ['외주창고_기타', '외주_재실사']

'''
[source, target] 정의 :
    외주창고 [source, target]:
        - 출고처리 [source, target]:
'''
df_MAT.loc[ (df_MAT['FCode'] == '0201') & 
           df_MAT['Created_by'].str.contains('GLV') , ['source', 'target'] ] = ['★외주창고★', '완제품창고_출고']


''' ---------------------------------------------------------------------------
5. 데이터 필터링 :
    (1) 가공데이터 - 메뉴 필터링
'''
# (1) 가공데이터 - 메뉴 필터링 : (Sankey 그래프화 제외 메뉴 )
df_MAT = df_MAT.loc[ ~(df_MAT['FCode'].isin(['0306'])) , :] # '0306'(신규라벨 발행)

''' ---------------------------------------------------------------------------
6. 가공데이터 저장
    (1) DB에 Table 쓰기 (df_MAT)
'''
# password 파싱 ("@" 제거)
password = urllib.parse.quote("dna@Kefic0")

# DB 연결 설정
conn = username+":"+ password + "@" + server + "/" + database                         
engine = sqlalchemy.create_engine('mssql+pymssql://' + conn)

# 데이터 저장
df_MAT.to_sql(name="df_MAT", con=engine, if_exists='append')














