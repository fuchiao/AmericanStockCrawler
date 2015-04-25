# -*- coding: utf-8 -*-
"""
Created on Thu Aug 21 23:23:53 2014

@author: zack
"""
import requests, re, string, sqlite3, time
import MySQLdb
import ConfigParser
import os.path
from datetime import date, datetime
import random

import os, sys
import time
from datetime import datetime
from multiprocessing import Process, Queue, Pool

import csv
def dateTrans(d):
    tmp = d.split('/')
    if len(tmp) < 3:
        return ""
    ret = tmp[2]+"-"+tmp[0]+"-"+tmp[1]
    return ret

def dateTrans2(d):
    tmp = d.replace(',', '').split()
    monthList = {"Jan":"1", "Feb":"2", "Mar":"3", "Apr":"4", "May":"5", "Jun":"6", "Jul":"7", "Aug":"8", "Sep":"9", "Oct":"10", "Nov":"11", "Dec":"12"}
    return tmp[2]+"-"+monthList[tmp[0]]+"-"+tmp[1]

def codeTrans(code):
    with open("codeTrans.csv", "r") as csvfile:
        codeReader = csv.reader(csvfile)
        for row in codeReader:
            if len(row) > 1 and row[0].lower() == code.lower():
                return row[1]
    return code

class dataHandler():
    def __init__(self):
        self.logQ = Queue()
        self.urlQ = Queue() # [code, url, func]
        self.sqlQ = Queue()
        self.sqlPriceQ = Queue()
        #sqlClear(self.logQ)

    def run(self, market='nyse'):
        logPool = Pool(1, logWrite, (self.logQ,))
        urlPool = Pool(10, webRequest, (self.urlQ, self.sqlQ, self.sqlPriceQ, self.logQ))
        sqlPool = Pool(1, sqlExec, (self.sqlQ, self.logQ, market))
        sqlPricePool = Pool(10, sqlExec, (self.sqlPriceQ, self.logQ, market))

        get_indexUrls(market, self.urlQ)
        #get_keyUrls_from_sql(market, self.urlQ, self.logQ)
        self.queueCheck()
        self.flushBuffer()
        return

    def flushBuffer(self):
        self.sqlQ.put(["flush",[]])
        for i in range(5):
            self.sqlPriceQ.put(["flush",[]])
        time.sleep(2)
        self.logQ.put("flush")
        time.sleep(30)

    def queueCheck(self):
        recheck = 3
        while True:
            if recheck == 0:
                break
            time.sleep(5)
            if self.urlQ.empty() and self.sqlQ.empty() and self.logQ.empty() and self.sqlPriceQ.empty():
                recheck -= 1
            else:
                recheck = 3
            self.logQ.put("urlQ size: "+str(self.urlQ.qsize()))
            self.logQ.put("sqlQ size: "+str(self.sqlQ.qsize()))
            self.logQ.put("sqlPriceQ size: "+str(self.sqlPriceQ.qsize()))
            self.logQ.put("logQ size: "+str(self.logQ.qsize()))

def get_keyUrls_from_sql(market, urlQ, logQ):   #market = "nasdaq" or "nyse"
    conn, c = sqlConnInit(logQ)
    conn.select_db(market)
    c.execute('''select * from updateList where lastUpdate!=DATE(NOW()) order by lastUpdate''')
    updateLogs = c.fetchall()
    logQ.put("update "+str(len(updateLogs))+" urls")
    for i in updateLogs:
        if i[1] == 'historicalPrice':
            codeY = codeTrans(i[0])
            urlQ.put([i[0], 'http://finance.yahoo.com/q/hp?s='+codeY+'+Historical+Prices', 'historicalPrice'])
        elif i[1] == 'dividendHistory':
            urlQ.put([i[0], 'http://dividata.com/stock/'+i[0]+'/dividend', 'dividendHistory'])
        elif i[1] == 'splitHistory':
            urlQ.put([i[0], 'http://getsplithistory.com/'+i[0], 'splitHistory'])
        elif i[1] == 'summary':
            code = codeTrans(i[0])
            urlQ.put([i[0], 'http://finance.yahoo.com/q?s='+codeY, 'summary'])
        else:
            raise Exception('Never get here')

def get_indexUrls(market, urlQ):
    for ch in string.ascii_uppercase:
        indexUrl = "http://eoddata.com/stocklist/"+market.upper()+"/"+ch+".htm"
        urlQ.put(["", indexUrl, "get_keyUrls"])

def get_keyUrls(text, code, urlQ, sqlQ):
    r = re.findall(">([A-Z.-]+)</A></td><td>([^<]+)</td>", text)
    for i in r:
        codeY = codeTrans(i[0])
        sqlQ.put(["codeList", i])
        urlQ.put([i[0], 'http://finance.yahoo.com/q/hp?s='+codeY+'+Historical+Prices', 'historicalPrice'])
        urlQ.put([i[0], 'http://dividata.com/stock/'+i[0]+'/dividend', 'dividendHistory'])
        urlQ.put([i[0], 'http://getsplithistory.com/'+i[0], 'splitHistory'])
        urlQ.put([i[0], 'http://finance.yahoo.com/q?s='+codeY, 'summary'])

def summary(text, code, sqlQ):
    r =re.findall("<td class=\"yfnc_tabledata1\">(.+?)</td>", text)
    if len(r) < 15:
        return

    d = [code]
    if r[0] == 'N/A':
        d.append(None)
    else:
        d.append(r[0].replace(',',''))
    if r[1] == 'N/A':
        d.append(None)
    else:
        d.append(r[1].replace(',',''))
    tmp = re.sub('<[^>]*>', '', r[2])
    if tmp == 'N/A':
        d.append(None)
    else:
        d.append(tmp)
    tmp = re.sub('<[^>]*>', '', r[3])
    if tmp == 'N/A':
        d.append(None)
    else:
        d.append(tmp)
    if r[4] == 'N/A':
        d.append(None)
    else:
        d.append(r[4].replace(',',''))
    if r[5] == 'N/A':
        d.append(None)
    else:
        d.append(r[5].replace(',',''))
    tmp = re.sub('<[^>]*>', '', r[6])
    if tmp == 'N/A':
        d.append(None)
    else:
        d.append(tmp)
    tmp = re.sub('<[^>]*>', '', r[7])
    if tmp == 'N/A':
        d.append(None)
    else:
        d.append(tmp)
    tmp = re.sub('<[^>]*>', '', r[8])
    if tmp == 'N/A':
        d.append(None)
    else:
        d.append(tmp)
    tmp = re.sub('<[^>]*>', '', r[9].replace(',',''))
    if tmp == 'N/A':
        d.append(None)
    else:
        d.append(tmp)
    if r[10] == 'N/A':
        d.append(None)
    else:
        d.append(r[10].replace(',',''))
    tmp = re.sub('<[^>]*>', '', r[11])
    if tmp == 'N/A':
        d.append(None)
    else:
        d.append(tmp)
    if r[12] == 'N/A':
        d.append(None)
    else:
        d.append(r[12].replace(',',''))
    if r[13] == 'N/A':
        d.append(None)
    else:
        d.append(r[13].replace(',',''))
    tmp = re.findall("([\d\.%]+)", r[14])
    if len(tmp) == 2:
        d.append(tmp[0])
    else:
        d.append(None)
    sqlQ.put(['summary', d])
    sql = ["updateList", [code, "summary"]]
    sqlQ.put(sql)


def historicalPrice(text, code, urlQ):
    r = re.findall("<p><a href=\"(.+)\"><img src=\"http://l.yimg.com/a/i/us/fi/02rd/spread.gif\"", text)
    if len(r) == 0: # no historical price
        return

    urlcsv = r[0]
    urlQ.put([code, urlcsv, "historicalPriceCsv"])

def historicalPriceCsv(text, code, sqlQ, sqlPriceQ):
    dateStr = str(date.today())
    r = re.findall("[\d\-.]+", text)
    if len(r) % 7 != 0:
        return
    for i in range(0, len(r), 7):
        sql = 'insert ignore into historicalPrice(code, date, open, high, low, close, volumn, adjClose) values (\''+code+'\',\''+r[i]+'\','+r[i+1]+','+r[i+2]+','+r[i+3]+','+r[i+4]+','+r[i+5]+','+r[i+6]+')'
        sql = ["historicalPrice", [code, r[i], r[i+1], r[i+2], r[i+3], r[i+4], r[i+5], r[i+6]]]
        sqlPriceQ.put(sql)

    sql = "update updateList SET lastUpdate = \""+str(date.today())+"\" WHERE code = \""+code+"\" AND tableName = \"historicalPrice\""
    sql = "insert into updateList(code, tableName, lastUpdate) values (\""+code+"\",\"historicalPrice\",\""+dateStr+"\") ON DUPLICATE KEY UPDATE lastUpdate=\""+dateStr+"\""
    sql = ["updateList", [code, "historicalPrice"]]
    sqlQ.put(sql)

def splitHistory(text, code, sqlQ):
    dateStr = str(date.today())

    a = text.find("<table")
    b = text.find("</table>")
    r = text[a:b]
    data = []
    while len(r) > 0:
        a = r.find("<")
        if a > 0:
            data[-1][-1] += r[:a]
            r = r[a:]
        else:
            if r.startswith("<tr"):
                data.append([])
            elif r.startswith("<td"):
                data[-1].append("")
            b = r.find(">")
            r = r[b+1:]
    if len(data) > 2:
        del data[0]
        del data[-1]
        r = []
        for i in range(len(data)):
            r.append([code, data[i][0], data[i][1], data[i][2], data[i][3], data[i][4].replace('%', '')])
            tmp = data[i][0].split()
            monthList = {"Jan":"1", "Feb":"2", "Mar":"3", "Apr":"4", "May":"5", "Jun":"6", "Jul":"7", "Aug":"8", "Sep":"9", "Oct":"10", "Nov":"11", "Dec":"12"}
            r[-1][1] = tmp[2]+"-"+monthList[tmp[0]]+"-"+tmp[1].replace(',', '')
            r[-1][4] = data[i][3].split()[0]
            if r[-1][3] == "-":
                r[-1][3] = "0"
            if r[-1][4] == "-":
                r[-1][4] = "0"
            if r[-1][5] == "-":
                r[-1][5] = "0"
            r[-1][3] = r[-1][3].replace(",","")
            r[-1][4] = r[-1][4].replace(",","")
            r[-1][5] = r[-1][5].replace(",","")
        for i in r:
            sql = 'insert ignore into splitHistory(code, date, ratio, priceClose, priceBefore, priceChangePercentage) values (\''+i[0]+'\',\''+i[1]+'\',\''+i[2]+'\','+i[3]+','+i[4]+','+i[5]+')'
            sql = ["splitHistory", [i[0], i[1], i[2], i[3], i[4], i[5]]]
            sqlQ.put(sql)

    sql = "update updateList SET lastUpdate = \""+str(date.today())+"\" WHERE code = \""+code+"\" AND tableName = \"splitHistory\""
    sql = "insert into updateList(code, tableName, lastUpdate) values (\""+code+"\",\"splitHistory\",\""+dateStr+"\") ON DUPLICATE KEY UPDATE lastUpdate=\""+dateStr+"\""
    sql = ["updateList", [code, "splitHistory"]]
    sqlQ.put(sql)

def dividendHistory(text, code, sqlQ):
    dateStr = str(date.today())

    r = re.findall("<li><p>([\w ,]+)</p> <p>\$([\d+\.]+)</p></li>", text)
    data = [[code, dateTrans2(i[0]), i[1]] for i in r]
    for i in data:
        sql = 'insert ignore into dividendHistory(code, date, amount) values (\''+i[0]+'\',\''+i[1]+'\','+i[2]+')'
        sql = ["dividendHistory", [i[0], i[1], i[2]]]
        sqlQ.put(sql)

    sql = "update updateList SET lastUpdate = \""+str(date.today())+"\" WHERE code = \""+code+"\" AND tableName = \"dividendHistory\""
    sql = "insert into updateList(code, tableName, lastUpdate) values (\""+code+"\",\"dividendHistory\",\""+dateStr+"\") ON DUPLICATE KEY UPDATE lastUpdate=\""+dateStr+"\""
    sql = ["updateList", [code, "dividendHistory"]]
    sqlQ.put(sql)

def webRequest(urlQ, sqlQ, sqlPriceQ, logQ):
    proxies = {"http":"http://proxy.hinet.net:80"}
    headers = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", 
               "User-Agent":"Mozilla/5.0",
               "Accept-Encoding":"gzip,deflate,sdch",
               "Accept-Language":"zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4"}
    dispatcher = {"dividendHistory": dividendHistory,
                  "historicalPrice": historicalPrice,
                  "historicalPriceCsv": historicalPriceCsv,
                  "splitHistory": splitHistory,
                  "summary": summary,
                  "get_keyUrls": get_keyUrls}
    while True:
        try:
            u = urlQ.get()
            code = u[0]
            url = u[1]
            funcName = u[2]
            while sqlPriceQ.qsize() > 100000:
                time.sleep(10)
                logQ.put("wait 10 sec, sqlQueue full")
            if funcName in ["dividendHistory", "historicalPrice", "splitHistory", "summary"]:
                sql = ["newItem", [code, funcName]]
                sqlQ.put(sql)
            logQ.put('get '+url)
            tStart = time.time()
            r = requests.get(url, headers = headers, proxies = proxies)
            tStop = time.time()
            if len(r.text) < 50:
                raise Exception(r.text)
            if r.text == "Retry later\n":
                raise Exception("RetryLater")
        except Exception as e:
            logQ.put(url+"\n"+str(e))
        else:
            logQ.put(url+" "+str(tStop-tStart)+"sec")
            if  funcName == "historicalPrice":
                dispatcher[funcName](r.text, code, urlQ)
            elif funcName == "get_keyUrls":
                dispatcher[funcName](r.text, code, urlQ, sqlQ)
            elif funcName == "historicalPriceCsv":
                dispatcher[funcName](r.text, code, sqlQ, sqlPriceQ)
            elif funcName == "summary":
                dispatcher[funcName](r.text, code, sqlQ)
            else:
                dispatcher[funcName](r.text, code, sqlQ)
    raise Exception("url NEVER GET HERE")

def logWrite(logQ):
    print str(datetime.now()) + ", Start logQueue"
    while True:
        s = logQ.get()
        print str(datetime.now()) + ", " + s
        if s == "flush":
            sys.stdout.flush()
    raise Exception("log NEVER GET HERE")

def sqlExec(sqlQ, logQ, market):
    #c.execute('''drop table if exists urlList''')
    #c.execute('''drop table if exists codeList''')
    sqlPattern = {'newItem':"insert ignore into updateList(code, tableName, lastUpdate) values (%s,%s,\"0000-00-00\")",
#                  'updateList':"insert into updateList(code, tableName, lastUpdate) values (%s,%s,\"0000-00-00\") ON DUPLICATE KEY UPDATE lastUpdate=DATE(NOW())",
                  'updateList':"update updateList SET lastUpdate=DATE(NOW()) WHERE code = %s AND tableName = %s",
                  'codeList':"insert ignore into codeList(code, name) values (%s,%s)",
                  'dividendHistory':'insert ignore into dividendHistory(code, date, amount) values (%s,%s,%s)',
                  'splitHistory':'insert ignore into splitHistory(code, date, ratio, priceClose, priceBefore, priceChangePercentage) values (%s,%s,%s,%s,%s,%s)',
                  'historicalPrice':'insert ignore into historicalPrice(code, date, open, high, low, close, volumn, adjClose) values (%s,%s,%s,%s,%s,%s,%s,%s)',
                  'summary':'replace into summary(code, prevClose, open, bid, ask, targetEst, beta, nextEarningDate, dayRange, yearRange, vol, avgVol, marketCap, pe, eps, divYield) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                  'flush':''}
    targetTable = "flush"
    data = []
    conn, c = sqlConnInit(logQ)
    conn.select_db(market)
    logQ.put("Start sqlQueue")
    while True:
        try:
            sql = sqlQ.get()    # [tableName, val list]
            if targetTable == "flush":
                targetTable = sql[0]
                del data[:]
            elif targetTable != sql[0] or len(data) >= 2000:
                tStart = time.time()
                c.executemany(sqlPattern[targetTable], data)
                tStop = time.time()
                if targetTable == 'summary':
                    print data
                conn.commit()
                logQ.put("commit "+targetTable+" "+str(len(data))+" "+str(tStop-tStart)+" sec")
                if sql[0] == "flush":
                    logQ.put("sqlProcess flush")
                    time.sleep(30)
                targetTable = sql[0]
                del data[:]
            data.append(sql[1])
            """
        except MySQLdb.IntegrityError as e:
            if e.args[0] == 1062:
                pass
            else:
                logQ.put(sql+"\n"+str(e))
            """
        except (AttributeError, MySQLdb.OperationalError):
            conn, c = sqlConnInit(logQ)
            conn.select_db(market)
            c.executemany(sqlPattern[targetTable], data)
            conn.commit()
            logQ.put("commit "+targetTable+" "+str(len(data)))
        except Exception as e:
            logQ.put(sqlPattern[targetTable])
            logQ.put(str(e))
    raise Exception("sql NEVER GET HERE")

def sqlClear(logQ):
    conn, c = sqlConnInit(logQ)
    for i in ["nasdaq", "nyse"]:
        c.execute("drop database if exists "+i)
        c.execute("create database if not exists "+i)
        conn.select_db(i)
        c.execute('''create table if not exists codeList (code CHAR(20) UNIQUE, name CHAR(50))''')
        c.execute('''create table if not exists updateList (code CHAR(20) , tableName CHAR(20), lastUpdate DATE, CONSTRAINT unq UNIQUE (code, tableName))''')
        c.execute('''create table if not exists historicalPrice (code CHAR(20), date DATE, open REAL, high REAL, low REAL, close REAL, volumn INT, adjClose REAL, CONSTRAINT unq UNIQUE (code, date))''')
        c.execute('''create table if not exists splitHistory (code CHAR(20), date DATE, ratio CHAR(20), priceClose REAL, priceBefore REAL, priceChangePercentage REAL, CONSTRAINT unq UNIQUE (code, date))''')
        c.execute('''create table if not exists dividendHistory (code CHAR(20), date DATE, amount REAL, CONSTRAINT unq UNIQUE (code, date))''')
        c.execute('''create table if not exists summary (code CHAR(20), prevClose REAL, open REAL, bid CHAR(20), ask CHAR(20), targetEst REAL, beta REAL, nextEarningDate CHAR(30), dayRange CHAR(20), yearRange CHAR(20), vol INT, avgVol INT, marketCap CHAR(10), pe REAL, eps REAL, divYield REAL, PRIMARY KEY(code))''')
    conn.commit()
    logQ.put("sqlInit")

def sqlConnInit(logQ):
    config = ConfigParser.ConfigParser()
    config.read("config")
    ssl = {"ca":config.get('mysql', 'ssl_ca'), "cert":config.get('mysql', 'ssl_cert'), "key":config.get('mysql', 'ssl_key')}
    while True:
        try:
            logQ.put("Connecting")
            conn = MySQLdb.connect(host=config.get('mysql', 'ip'), user=config.get('mysql', 'user'), passwd=config.get('mysql', 'passwd'), ssl=ssl)
        except MySQLdb.OperationalError as e:
            logQ.put("Connection Error\n"+str(e))
        else:
            break
    c = conn.cursor()
    return conn, c

def main():
    app = dataHandler()
    app.run('nyse')
    app.run('nasdaq')
    raise Exception("NEVER GET HERE")

if __name__ == "__main__":
    """
    if not os.path.exists("config"):
        config = ConfigParser.RawConfigParser()
        config.add_section('mysql')
        config.set('mysql', 'ip', raw_input("mysql_ip: "))
        config.set('mysql', 'user', raw_input("mysql_user: "))
        config.set('mysql', 'passwd', raw_input("mysql_passwd: "))
        with open("stockCrawler.cfg", "wb") as cfgFile:
            config.write(cfgFile)
    """
    main()

