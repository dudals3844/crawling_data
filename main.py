from selenium import webdriver
import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import xlsxwriter
import re
import datetime
import sys
import sqlite3
from multiprocessing import Pool
sys.path.append('C:/Users/PC/PycharmProjects/data/')

class CrawlingData():

    def __init__(self):
        # 테스트 속도를 위해 잠시 주석처리
        #self.delete_file(file_dr='C:/Users/PC/PycharmProjects/systemtrading_platform/db/상장회사.xls')
        #self.delete_file(file_dr="C:/Users/PC/Downloads/data.xls")
        # #self.download_company_data()
        # self.con = sqlite3.connect("C:/Users/PC/PycharmProjects/systemtrading_platform/db/DayPrice.db")
        print(company_df)
        self.crawling_all_company_price_data(company_df)
        #self.crwling_all_company_price_data_update(company_df)

    # 상장회사 엑셀 다운로드
    def download_company_data(self):
        driver = webdriver.Chrome("C:/Users/PC/PycharmProjects/systemtrading_platform/driver/chromedriver.exe")
        driver.implicitly_wait(2)
        driver.get('http://marketdata.krx.co.kr/mdi#document=040601')
        driver.find_element_by_xpath('//*[@id="6f4922f45568161a8cdf4ad2299f6d23"]/button[2]').click()
        # 다운될때까지 기다리기
        time.sleep(3)
        self.change_file_name_and_directory(origin_dr="C:/Users/PC/Downloads/data.xls",
                                            change_dr='C:/Users/PC/PycharmProjects/systemtrading_platform/db/상장회사.xls')

        driver.close()

    # 파일 삭제
    def delete_file(self, file_dr):
        if os.path.isfile(file_dr):
            os.remove(file_dr)

    # 코드번호 형식 맞추기
    def standard_code(self, code):
        code = "{0:0>6}".format(code)  # 오류안나게 종목코드 6자리 맞춰줌
        return code

    # 파일 이름과 폴더 경로 변경
    def change_file_name_and_directory(self, origin_dr, change_dr):
        os.rename(origin_dr, change_dr)


    # 데이터 프레임을 엑셀로 저장
    def save_dataframe_as_excel(self, save_folder, dataframe,file_name):
        file_dr = save_folder + file_name + '.xlsx'
        writer = pd.ExcelWriter(file_dr, engine='xlsxwriter')
        dataframe.to_excel(writer, sheet_name='price')
        writer.close()

    #sqlite3에 저장
    def saveDataFrameAsDb(self, dataFrame, fileName):
        dataFrame.to_sql(fileName, self.con,if_exists='replace')

    def saveDataFrameToCsv(self, dataFrame, fileName):
        dataFrame.to_csv('C:/Users/PC/PycharmProjects/systemtrading_platform/db/pricecsv/'+fileName+'.csv', mode='w')


    # 상장된 회사들 전체 가격 저장
    def crawling_all_company_price_data(self, company_df):
        pattern = re.compile("(\d+)")
        for i in range(0, len(company_df)):
            stock = pd.DataFrame()
            code = self.standard_code(company_df['종목코드'][i])

            url = "https://finance.naver.com/item/sise_day.nhn?code={}&page={}"
            try:
                last = pattern.findall(
                    bs(requests.get(url.format(code, 1)).text, 'html.parser').find("td",class_='pgRR').find("a")['href'])[-1]
            except:
                last = 1

            print(code+' 크롤링 중')
            for cnt in range(int(last),0,-1):
                data = pd.read_html(url.format(code,cnt))[0].dropna()
                data = data.sort_index(ascending=False)
                data.reset_index(drop=True, inplace = True)
                stock = stock.append(data)

            stock.reset_index(drop=True, inplace=True)

            self.saveDataFrameToCsv(dataFrame=stock, fileName=code)


    def crwling_all_company_price_data_update(self, company_df):

        for i in range(0, len(company_df)):
            try:
                tmp_df = None

                code = self.standard_code(company_df['종목코드'][i])
                tableName = 'DAY_'+code+'_TB'
                stock_df = pd.read_sql('select * from ' +tableName, con=self.con)
                url = "https://finance.naver.com/item/sise_day.nhn?code={}"

                tmp_df = pd.read_html(url.format(code))[0].dropna()
                tmp_df = tmp_df.sort_index(ascending=False)
                tmp_df.reset_index(drop=True, inplace=True)
                print(code)
                stock_df = stock_df.append(tmp_df)
                # tmp_df.drop(['Unnamed: 0'],axis='columns', inplace=True)
                stock_df.drop_duplicates(['날짜'], inplace=True)
                stock_df.reset_index(drop=True, inplace=True)
                stock_df.dropna(axis = 'columns', inplace = True)
                self.saveDataFrameToCsv(dataFrame = stock_df, fileName = code)
            except:
                pass


def standard_code(code):
    code = "{0:0>6}".format(code)  # 오류안나게 종목코드 6자리 맞춰줌
    return code

# 엑셀 파일로 데이터 프레임 읽기
def read_excel_to_dataframe(file_dr):
    df = pd.read_excel(file_dr)
    return df

def saveDataFrameToCsv(dataFrame, fileName):
    dataFrame.to_csv('C:/Users/PC/PycharmProjects/systemtrading_platform/db/pricecsv/'+fileName+'.csv', mode='w')

# 상장된 회사들 전체 가격 저장
def crawling_all_company_price_data(code):
    pattern = re.compile("(\d+)")
    stock = pd.DataFrame()
    code = standard_code(code)

    url = "https://finance.naver.com/item/sise_day.nhn?code={}&page={}"
    try:
        last = pattern.findall(
            bs(requests.get(url.format(code, 1)).text, 'html.parser').find("td",class_='pgRR').find("a")['href'])[-1]
    except:
        last = 1

    print(code+' 크롤링 중')
    for cnt in range(int(last),0,-1):
        data = pd.read_html(url.format(code,cnt))[0].dropna()
        data = data.sort_index(ascending=False)
        data.reset_index(drop=True, inplace = True)
        stock = stock.append(data)

    stock.reset_index(drop=True, inplace=True)

    saveDataFrameToCsv(dataFrame=stock, fileName=code)

if __name__ =="__main__":
    company_df = read_excel_to_dataframe('C:/Users/PC/PycharmProjects/systemtrading_platform/db/상장회사.xls')
    company_series = company_df['종목코드']
    company_list = list(company_series)
    pool = Pool(processes=8)
    pool.map(crawling_all_company_price_data, company_list)
    pool.close()
    pool.join()
