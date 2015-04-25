import csv
import MySQLdb
import ConfigParser

cpi = []

def checkCpi(func):
    def load(*args, **kwargs):
        if len(cpi) > 0:
            return func(*args, **kwargs)
        with open("CPIAUCSL.csv", "r") as csvfile:
            csvReader = csv.reader(csvfile)
            next(csvReader, None)
            for row in csvReader:
                spt = row[0].split('-')
                cpi.append([int(spt[0]), int(spt[1]), float(row[1])])
        return func(*args, **kwargs)
    return load
    
@checkCpi
def getCpi(date):
    spt = date.split('-')
    y = int(spt[0])
    m = int(spt[1])
    for i in cpi:
        if i[0] > y:
            return i[2]
        elif i[0] == y and i[1] >= m:
            return i[2]
        else:
            r = i[2]
    return r

def sqlConnInit():
    config = ConfigParser.ConfigParser()
    config.read("config")
    ssl = {"ca":config.get('mysql', 'ssl_ca'), "cert":config.get('mysql', 'ssl_cert'), "key":config.get('mysql', 'ssl_key')}
    while True:
        try:
            print "Connecting"
            conn = MySQLdb.connect(host=config.get('mysql', 'ip'), 
                                   user=config.get('mysql', 'user'), 
                                   passwd=config.get('mysql', 'passwd'), 
                                   ssl=ssl)
        except MySQLdb.OperationalError as e:
            print "Connection Error\n"+str(e)
        else:
            break
    c = conn.cursor()
    return conn, c

outputList = None

def dumpCodeNamePrice(c):
    global outputList
    sql = 'select codeList.code, codeList.name, summary.prevClose from codeList, summary where codeList.code = summary.code'
    c.execute(sql)
    dumpList = c.fetchall()
    outputList = map(list, dumpList)

def getMA(c, code, i):
    sql = 'select code, date, close from historicalPrice where code = %s order by date desc limit '+str(i)
    c.execute(sql, (code,))
    priceList = c.fetchall()
    print priceList
    ma = 0
    for j in priceList:
        ma += j[2]
    if len(priceList) > 0:
        ma /= len(priceList)
    else:
        ma = float('nan')
    return ma

def getDividendList(c, code, startFrom = '2000-01-01'):
    sql = 'select code, date, amount from dividendHistory where code = %s and date >= %s order by date desc'
    c.execute(sql, (code,startFrom,))
    dumpList = c.fetchall()
    divCpiList = map(list, dumpList)
    for i in range(len(dumpList)):
        divCpiList[i].append(getCpi(str(dumpList[i][1])))
    return divCpiList

def main(market = 'nyse'):
    global outputList
    conn, c = sqlConnInit()
    conn.select_db(market)
    dumpCodeNamePrice(c)
    for i in range(len(outputList)):
        outputList[i].append(getMA(c, outputList[i][0], 5))
        outputList[i].append((outputList[i][2] - outputList[i][3]) / outputList[i][3])
        divList = getDividendList(c, outputList[i][0], '2000-01-01')
        print divList
        
        divAry = [j[2] for j in divList]
        if len(divAry) < 1:
            continue
        outputList[i].append(min(divAry)/outputList[i][2])
        outputList[i].append(divAry[0]/outputList[i][2])
        if len(divList) < 2:
            continue
        delta = divList[0][1] - divList[-1][1]
        deltaYears = delta.days / 365.0
        if divList[-1][2] == 0 or divList[-1][3] == 0:
            divGrowth = -1
        else:
            divGrowth = (divList[0][2] / divList[-1][2] / divList[-1][3] * divList[0][3]) ** (1/deltaYears)
        outputList[i].append(divGrowth)
        outputList[i].append(0)
        for j in range(1, len(divList)):
            delta = divList[j-1][1] - divList[j][1]
            if delta.days < 110:
                outputList[i][-1] = j
            else:
                break
        outputList[i].append(0)
        for j in range(1, len(divList)):
            if divList[j-1][2] >= divList[j][2]:
                outputList[i][-1] = j
            else:
                break

    with open(market+".csv", "w") as f:
        f.write("code, name, stockPrice, MA, price2ma, minDivRate, avgDivRate, divGrowth, divContinuous, divNoReduce\n")
        for i in range(len(outputList)):
            if len(outputList[i]) < 10:
                continue
            f.write(outputList[i][0]+", "+outputList[i][1]+", "+str(outputList[i][2])+", "+str(outputList[i][3])+ \
                  ", "+str(outputList[i][4])+", "+str(outputList[i][5])+", "+str(outputList[i][6])+ \
                  ", "+str(outputList[i][7])+", "+str(outputList[i][8])+", "+str(outputList[i][9])+"\n")
            print "code: "+outputList[i][0]+", name: "+outputList[i][1]+", stockPrice: "+str(outputList[i][2])+", MA: "+str(outputList[i][3])+ \
                  ", price2ma: "+str(outputList[i][4])+", minDivRate: "+str(outputList[i][5])+", avgDivRate: "+str(outputList[i][6])+ \
                  ", divGrowth:"+str(outputList[i][7])+", divContinuous: "+str(outputList[i][8])+", divNoReduce: "+str(outputList[i][9])
    return

    print getCpi('1946-1-2')
    print getCpi('1947-1-2')
    print getCpi('1947-2-2')
    print getCpi('2014-11-23')
    print getCpi('2014-12-23')
    print getCpi('2015-3-2')
    
if __name__ == '__main__':
    main('nyse')
    main('nasdaq')

