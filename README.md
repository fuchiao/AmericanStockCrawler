# AmericanStockCrawler
##get code list
##get historical price and stock summary
##get split history
##get dividend history
##CPI
     manually
     monthly
     http://research.stlouisfed.org/fred2/series/CPIAUCSL/downloaddata?cid=9

##alg
     以CPI調整現金股利
     

##func
    @1: 連續@1年發放現金股利
    @2: 連續@2年現金股利不減少
    @3: 近@3年現金股利成長率大於 X%
    @4: @4年來最小現金股利除以最新股價大於 X%
    @5: 最新現金股利除以最新股價大於 X%
    @6: 股價大於@6日 MA
    output db
    code, name, stockPrice, @6MA, ([4]-[3])/[3], 10年來最小現金股利除以他們的最新股價, 最新的現金股利除以他們的最新股價, @3年股利成長率(CPI調整), 現金股利連續不減年數, 現金股利連續發放年數, alpha?, beta?, alpha/beta?, 年化波動率, 還權股價歷史報酬率

