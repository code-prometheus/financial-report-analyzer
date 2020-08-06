import requests
import re

FR = {}

def get_general_report(stock_id, year, report_id, item_ids):
    #try:
        url = 'http://money.finance.sina.com.cn/corp/go.php/vFD_' + report_id + '/stockid/' + stock_id + '/ctrl/' + year + '/displaytype/4.phtml'
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        print(format_web_page(r.text))
        r.encoding = r.apparent_encoding
        date = re.search(r'报[告|表]日期.*(>(\d{4}-\d{2}-\d{2})<)', format_web_page(r.text), flags=0)
        if date:
            report_dates = re.findall(r'>(\d{4}-\d{2}-\d{2})<', date.group(0), flags=0)
            if report_dates:
                for d in report_dates:
                    item = {'report_date': d}
                    if not d in FR.keys():
                        FR[d] = item
                match_result(format_web_page(r.text), item_ids)
        return FR
    #except:
        return "err"


def format_web_page(content):
    content = content.lower()
    content = content.replace('\r', '')
    content = content.replace('\n', '')
    content = content.replace('\t', '')
    content = content.replace('\"', '')
    content = content.replace(' ', '')
    content = content.replace(',', '')
    content = content.replace('<tr', '')
    content = content.replace('</tr>', '\r\n')
    content = content.replace('<td>', '>')
    content = content.replace('</td>', '<')
    return content


def match_result(content, match_items):
    for item in match_items:
        m_item = re.search(item[0]+r'.*>(-?(\d{1,20}(\.\d{1,4})?)|--)<', content)
        if m_item:
            result = re.findall(r'>(-?(\d{1,20}(\.\d{1,4})?)|--)<', m_item.group(0))
            if result:
                i = 0
                for v in FR.values():
                    if not result[i][0]=='--':
                        v[item[1]] = round(float(result[i][0])/float(item[2]), 1)
                        i = i+1

#get_general_report('600975', '2018', 'ProfitStatement', {('(归属于母公司所有者的净利润|归属于母公司的净利润)', 'net_profit_market', 1), ('销售费用', 'S&D_expense', 1), ('财务费用', 'finance_expense', 1), ('管理费用', 'D&A_expense', 1), ('研发费用', 'R&D_expense', 1)})
get_general_report('600975', '2018', 'BalanceSheet', {('资产总计', 'asset', 1), ('>流动负债合计<', 'debt', 1),('(归属于母公司股东权益合计|归属于母公司股东的权益)','net_asset_market', 1)})
#get_general_report('600975', '2018', 'CashFlow', {('经营活动产生的现金流量净额', 'cash', 1), ('销售费用', 'S&D expense', 1), ('财务费用', 'finance_expense', 1)})
print(FR)