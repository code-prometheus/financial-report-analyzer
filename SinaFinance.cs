using System;
using System.Linq;
using System.Xml.Linq;
using System.Text.RegularExpressions;
using System.Collections;
using System.Threading;
using System.Data;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using StockDeamon;

namespace FinancialReportAnalyze
{
    class SinaFinance
    {
        XDocument NoTradeDates = new XDocument();
        XDocument settings = new XDocument();
        public string DataPath = System.Environment.CurrentDirectory;

        public SinaFinance(XDocument setting, XDocument notradeday)
        {
            settings = setting;
            NoTradeDates = notradeday;
            if (settings.Root.Element("DataPath") != null) DataPath = settings.Root.Element("DataPath").Value.ToString();
        }

        public bool IsTradeDay(DateTime date)
        {
            if (date.DayOfWeek == DayOfWeek.Sunday || date.DayOfWeek == DayOfWeek.Saturday)
            {
                return false;
            }
            var nonetradedates = from rec in NoTradeDates.Elements("dates").Elements("d") select (string)rec.Value;
            foreach (string dt in nonetradedates)
            {
                if (dt == date.ToString("yyyy-MM-dd")) return false;
            }
            return true;
        }

        public DateTime GetLastTradeDate(DateTime the_day)
        {
            TimeSpan one_day = new TimeSpan(1, 0, 0, 0);
            for (; !IsTradeDay(the_day); the_day = the_day - one_day) { };
            return the_day;
        }

        public DateTime GetNextTradeDate(DateTime the_day)
        {
            TimeSpan one_day = new TimeSpan(1, 0, 0, 0);
            for (; !IsTradeDay(the_day); the_day = the_day + one_day) { };
            return the_day;
        }

        public void BPnetGenerateTrainStockRecord()
        {
            ThreadPool.QueueUserWorkItem(new WaitCallback(BPnetGenerateTrainStockRecord), null);
        }

        void BPnetGenerateTrainStockRecord(object o)
        {
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            DateTime begin_date = GetQuarterReportDate(DateTime.Today, "b");
            for (int i = 0; i < 12; i++) begin_date = GetQuarterReportDate(begin_date, "b");
            ArrayList codes = new ArrayList();
            string sql = "select code from 年化财报 where 扣非净利润>0 and round(扣非净利润/净资产,6)>0.08 and 截止日期>=to_date('" + begin_date.ToString("yyyyMMdd") + "','yyyymmdd') having count(code)=12 group by code order by code";
            DataRowCollection rows = cd.GetRecords(sql);
            if (rows != null)
            {
                foreach(DataRow row in rows)
                {
                    codes.Add(row["code"].ToString());
                }
            }
            ArrayList my_codes = new ArrayList();
            foreach (string code in codes)
            {
                sql = "select round(扣非净利润/净资产,6) as roe from 年化财报 where code='" + code + "' order by 截止日期";
                rows = cd.GetRecords(sql);
                if (rows != null)
                {
                    double max_raise = 0;
                    double last_roe = double.Parse(rows[0]["roe"].ToString());
                    for (int i = 1; i < rows.Count; i++)
                    {
                        double roe = double.Parse(rows[i]["roe"].ToString());
                        double raise = Math.Abs((roe - last_roe) / last_roe);
                        last_roe = roe;
                        if (raise > max_raise) max_raise = raise;
                    }
                    if (max_raise < 0.2) my_codes.Add(code);
                }
            }
            codes = new ArrayList();
            foreach (string code in my_codes)
            {
                sql = "select count(distinct code) as ct from stockbase where i_id=(select distinct i_id from stockbase where code='" + code + "')";
                rows = cd.GetRecords(sql);
                if (rows != null)
                {
                    if (double.Parse(rows[0]["ct"].ToString()) > 10) codes.Add(code);
                }
            }
            foreach(string code in codes)
            {
                GenerateTrainingRecord(code);
            }
        }

        public ArrayList CrawlHS300FromSina()
        {
            ArrayList codes = new ArrayList();
            HttpSession info_page = new HttpSession();
            string content = "";
            string link, msg = "";
            for (int page = 1; page < 10; page++)
            {
                link = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=" + page + "&num=40&sort=symbol&asc=1&node=hs300&symbol=&_s_r_a=page";
                msg = info_page.HTTP_REQUEST(link, out content);
                if (msg == "OK")
                {
                    content = FormatWebPage(content);
                    content = content.Replace("code:", "\r\n");
                    MatchCollection mc = Regex.Matches(content, @"(\d{6})name");
                    foreach(Match m in mc)
                    {
                        codes.Add(m.Groups[1].Value);
                    }
                }
            }
            return codes;
        }

        string FormatWebPage(string content)
        {
            content = content.ToLower();
            content = content.Replace("\r", "");
            content = content.Replace("\n", "");
            content = content.Replace("\t", "");
            content = content.Replace("\t", "");
            content = content.Replace("\"", "");
            content = content.Replace(" ", "");
            content = content.Replace(",", "");
            content = content.Replace("<tr", "");
            content = content.Replace("</tr>", "\r\n");
            content = content.Replace("<td>", ">");
            content = content.Replace("</td>", "<");
            return content;
        }

        void Crawl_sina(object param)
        {
            ArrayList codes = GetAllStocks();
            for (int idx = 0; idx < codes.Count; idx++)
            {
                string code = codes[idx].ToString();
                GetStockFinacialReport(code);
            }
        }    ////新浪爬虫

        void GetStockFinacialReport(string code)
        {
            Dictionary<DateTime, Hashtable> report_base = new Dictionary<DateTime, Hashtable>();
            report_base = GetGeneralReport(code, "FinancialGuideLine", report_base);  ///新浪财务指标
            report_base = GetGeneralReport(code, "BalanceSheet", report_base);  ///新浪资产负债表
            report_base = GetGeneralReport(code, "ProfitStatement", report_base);  //新浪利润表
            report_base = GetGeneralReport(code, "CashFlow", report_base);   //新浪现金流表
            report_base = GetReportDate(code, report_base);
            if (report_base.Count > 0)
            {
                InsertIntoDB(code, report_base);
            }
            Thread.Sleep(3000);      ////歇一会儿
        }       ////新浪财务报表分析

        Dictionary<DateTime, Hashtable> GetGeneralReport(string code, string report_id, Dictionary<DateTime, Hashtable> report)   /////新浪通用报表匹配
        {
            HttpSession info_page = new HttpSession();
            string content = "";
            string link, msg = "";
            DateTime max_date = GetMaxUpdate(code);
            for (int year = DateTime.Today.Year; year >= DateTime.Today.Year - 5; year--)
            {
                if (year < max_date.Year) break;
                link = "http://money.finance.sina.com.cn/corp/go.php/vFD_" + report_id + "/stockid/" + code + "/ctrl/" + year + "/displaytype/4.phtml";
                msg = info_page.HTTP_REQUEST(link, out content);
                if (msg == "OK")
                {
                    content = FormatWebPage(content);
                    Match m_date = Regex.Match(content, @"报[告|表]日期.*(>(\d{4}-\d{2}-\d{2})<)");
                    if (m_date.Success)
                    {
                        Dictionary<int, DateTime> idx = new Dictionary<int, DateTime>();
                        string text = m_date.Groups[0].ToString().Replace("<", "\r\n");
                        MatchCollection mc_date = Regex.Matches(text, @"(\d{4}-\d{2}-\d{2})");
                        for (int i = 0; i < mc_date.Count; i++)
                        {
                            DateTime date = DateTime.Parse(mc_date[i].ToString());
                            idx.Add(i, date);
                            if (!report.ContainsKey(date))
                            {
                                Hashtable set = new Hashtable();
                                report.Add(date, set);
                            }
                        }
                        report = MatchResult(content, @"资产总计", "总资产", report, idx);
                        report = MatchResult(content, @"(归属于母公司股东权益合计|归属于母公司股东的权益)", "净资产", report, idx);
                        report = MatchResult(content, @"经营活动产生的现金流量净额", "现金流净额", report, idx);
                        report = MatchResult(content, @"(营业总收入|营业收入)", "营业收入", report, idx);
                        report = MatchResult(content, @"(归属于母公司所有者的净利润|归属于母公司的净利润)", "净利润", report, idx);
                        report = MatchResult(content, @"(扣除非经常性损益后的净利润\(元\))", "扣非净利润", report, idx);
                        report = MatchResult(content, @"销售费用", "期间费用", report, idx);
                        report = MatchResult(content, @"财务费用", "期间费用", report, idx);
                        report = MatchResult(content, @"管理费用", "期间费用", report, idx);
                        if (year < (DateTime.Today.Year - 1) && mc_date.Count < 4) break;    //////年不够5年
                    }
                }
            }
            report = report.OrderBy(p => p.Key).ToDictionary(p => p.Key, p => p.Value);
            return report;
        }

        Dictionary<DateTime, Hashtable> MatchResult(string content, string match_item, string brief_name, Dictionary<DateTime, Hashtable> report, Dictionary<int, DateTime> idx)
        {
            Match m_item = Regex.Match(content, match_item + @".*>(-?(\d{1,20}(\.\d{1,4})?)|--)<");
            if (m_item.Success)
            {
                content = m_item.Groups[0].ToString().Replace("<", "<\r\n");
                MatchCollection mc_items = Regex.Matches(content, @"((-?\d{1,20}(\.\d{1,4})?)|--)");
                for (int i = 0; i < mc_items.Count; i++)
                {
                    double value = 0;
                    if (mc_items[i].ToString() != "--") value = double.Parse(mc_items[i].ToString()) * 10000.0;
                    if (brief_name == "扣非净利润") value = value / 10000.0;
                    Hashtable set = report[idx[i]];
                    if (set.ContainsKey(brief_name)) set[brief_name] = (double)set[brief_name] + Math.Round(value, 2);
                    else set.Add(brief_name, Math.Round(value, 2));
                    report[idx[i]] = set;
                }
            }
            return report;
        }

        Dictionary<DateTime, Hashtable> GetReportDate(string code, Dictionary<DateTime, Hashtable> report)   ///新浪季报日期
        {
            HttpSession info_page = new HttpSession();
            string content = "";
            string link, msg = "";
            for (int quarter = 1; quarter <= 4; quarter++)
            {
                int month = 1, day = 1;
                string type = "";
                switch(quarter)
                {
                    case 1:
                        month = 3;day=31;
                        type = "yjdbg";
                        break;
                    case 2:
                        month = 6;day=30;
                        type = "zqbg";
                        break;
                    case 3:
                        month = 9;day=30;
                        type = "sjdbg";
                        break;
                    case 4:
                        month = 12;day=31;
                        type = "ndbg";
                        break;
                }
                link = "http://money.finance.sina.com.cn/corp/go.php/vCB_AllBulletin/stockid/" + code + ".phtml?ftype=" + type;
                msg = info_page.HTTP_REQUEST(link, out content);
                if (msg == "OK")
                {
                    content = FormatWebPage(content);
                    content = content.Replace("<br>", "\r\n");
                    content = content.Replace("<ul>", "\r\n");
                    MatchCollection mc_date = Regex.Matches(content, @"(\d{4}-\d{2}-\d{2})&nbsp");
                    if (mc_date.Count > 0)
                    {
                        foreach(Match m in mc_date)
                        {
                            DateTime release_date = DateTime.Parse(m.Groups[1].ToString());
                            int year;
                            if (quarter == 4) year = release_date.Year - 1;
                            else year = release_date.Year;
                            DateTime report_date = new DateTime(year, month, day);
                            if(report.ContainsKey(report_date))
                            {
                                Hashtable set = report[report_date];
                                if (!set.ContainsKey("公告日期")) set.Add("公告日期", release_date);
                                else set["公告日期"] = release_date;
                                report[report_date] = set;
                            }
                        }
                    }
                }
            }
            report = report.OrderBy(p => p.Key).ToDictionary(p => p.Key, p => p.Value);
            return report;
        }

        DateTime GetQuarterReportDate(DateTime date, string flag)
        {
            date = GetQuarterReportDate(date);   ////先把日期归到标准季报时间
            if (flag == "b") date = date.AddDays(-100);  ///减100天到上季度
            if (flag == "f") date = date.AddDays(1);   //加1天到下一季度
            return GetQuarterReportDate(date);
        }

        DateTime GetQuarterReportDate(DateTime date)
        {
            if (date.Month >= 1 && date.Month <= 3)
            {
                date = new DateTime(date.Year, 3, 31);
            }
            if (date.Month >= 4 && date.Month <= 6)
            {
                date = new DateTime(date.Year, 6, 30);
            }
            if (date.Month >= 7 && date.Month <= 9)
            {
                date = new DateTime(date.Year, 9, 30);
            }
            if (date.Month >= 10 && date.Month <= 12)
            {
                date = new DateTime(date.Year, 12, 31);
            }
            return date;
        }

        ArrayList GetAllStocks()
        {
            ArrayList result = new ArrayList();
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            DataRowCollection rows = cd.GetRecords("select distinct code from stockcode order by code");
            if (rows != null)
            {
                foreach (DataRow row in rows)
                {
                    string code = row["code"].ToString();
                    result.Add(code);
                }
            }
            return result;
        }

        DateTime GetMaxUpdate(string code)
        {
            DateTime result = new DateTime(2013, 1, 1);
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            DataRowCollection rows = cd.GetRecords("select max(截止日期) as 截止日期 from 财报 where code='" + code + "'");
            if (rows != null)
            {
                if (rows.Count > 0)
                {
                    string date = rows[0]["截止日期"].ToString();
                    if (date != "")
                    {
                        result = DateTime.Parse(date);
                    }
                }
            }
            return result;
        }

        void InsertIntoDB(string code,Dictionary<DateTime, Hashtable> report)
        {
            DateTime max_date = GetMaxUpdate(code);
            string sql = "";
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            foreach (DateTime date in report.Keys)
            {
                if (date <= max_date) continue;
                Hashtable set = report[date];
                if (set.Count < 8) continue;
                sql += "insert into 财报 (code,截止日期,公告日期";
                foreach (string item in set.Keys)
                {
                    if (!item.Contains("日期"))
                    {
                        sql += "," + item;
                    }
                }
                sql += ") values ('" + code + "',to_date('" + date.ToString("yyyy-MM-dd HH:mm:ss") + "','yyyy-mm-dd hh24:mi:ss'),to_date('" + DateTime.Parse(set["公告日期"].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "','yyyy-mm-dd hh24:mi:ss')";
                foreach (string item in set.Keys)
                {
                    if (!item.Contains("日期"))
                    {
                        sql += "," + set[item].ToString();
                    }
                }
                sql += ");";
            }
            if (sql != "") cd.DoSQL(sql);
        }

        void InsertIntoTTMDB(Dictionary<DateTime, Hashtable> report)
        {
            string sql = "";
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            foreach (DateTime date in report.Keys)
            {
                Hashtable set = report[date];
                string code = set["code"].ToString();
                sql += "insert into 年化财报 (code,截止日期,公告日期";
                foreach (string item in set.Keys)
                {
                    if (!(item.Contains("日期") || item.Contains("code")))
                    {
                        sql += "," + item;
                    }
                }
                sql += ") values ('" + code + "',to_date('" + date.ToString("yyyy-MM-dd HH:mm:ss") + "','yyyy-mm-dd hh24:mi:ss'),to_date('" + DateTime.Parse(set["公告日期"].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "','yyyy-mm-dd hh24:mi:ss')";
                foreach (string item in set.Keys)
                {
                    if (!(item.Contains("日期") || item.Contains("code")) )
                    {
                        sql += "," + set[item].ToString();
                    }
                }
                sql += ");";
            }
            if (sql != "") cd.DoSQL(sql);
        }

        public void GenerateReportTTM()
        {
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            cd.DoSQL("delete from 年化财报");
            DataRowCollection rows = cd.GetRecords("select distinct code from 财报 order by code");
            if (rows != null)
            {
                ArrayList codes = new ArrayList();
                foreach (DataRow row in rows)
                {
                    codes.Add(row["code"].ToString());
                }
                foreach (string code in codes)
                {
                    rows = cd.GetRecords("select * from 财报 where code='" + code + "'order by 截止日期,公告日期");
                    if (rows != null)
                    {
                        if (rows.Count < 5) continue;       ////少于5条记录年化不了
                        Dictionary<DateTime, Hashtable> report_base = new Dictionary<DateTime, Hashtable>();
                        foreach (DataRow row in rows)
                        {
                            if (double.Parse(row["营业收入"].ToString()) > 0 && double.Parse(row["净资产"].ToString()) > 0 && double.Parse(row["总资产"].ToString()) > 0)
                            {
                                Hashtable set = new Hashtable();
                                set.Add("code", row["code"].ToString());
                                set.Add("公告日期", DateTime.Parse(row["公告日期"].ToString()));
                                set.Add("现金流净额", double.Parse(row["现金流净额"].ToString()));
                                set.Add("净利润", double.Parse(row["净利润"].ToString()));
                                set.Add("期间费用", double.Parse(row["期间费用"].ToString()));
                                set.Add("营业收入", double.Parse(row["营业收入"].ToString()));
                                set.Add("净资产", double.Parse(row["净资产"].ToString()));
                                set.Add("总资产", double.Parse(row["总资产"].ToString()));
                                if (double.Parse(row["扣非净利润"].ToString()) != 0)
                                {
                                    set.Add("扣非净利润", double.Parse(row["扣非净利润"].ToString()));
                                }
                                else
                                {
                                    set.Add("扣非净利润", double.Parse(row["净利润"].ToString()));
                                }
                                report_base.Add(DateTime.Parse(row["截止日期"].ToString()), set);
                            }
                        }
                        report_base = report_base.OrderByDescending(p => p.Key).ToDictionary(p => p.Key, p => p.Value);
                        DateTime this_quarter = report_base.Keys.ElementAt(0);
                        for (int i = 1; i < report_base.Count; i++)                             ////保证report连续有效
                        {
                            DateTime next_quarter = report_base.Keys.ElementAt(i);
                            if (GetQuarterReportDate(this_quarter, "b") == next_quarter)
                            {
                                this_quarter = next_quarter;
                                continue;
                            }
                            else
                            {
                                Dictionary<DateTime, Hashtable> tmp = new Dictionary<DateTime, Hashtable>();
                                for (int j = 0; j < i; j++)
                                {
                                    tmp.Add(report_base.Keys.ElementAt(j), report_base.Values.ElementAt(j));
                                }
                                report_base = tmp;
                                break;
                            }
                        }
                        Dictionary<DateTime, Hashtable> report_ttm = GenerateReportTTM(report_base);
                        InsertIntoTTMDB(report_ttm);
                    }
                }
            }
        }

        Dictionary<DateTime, Hashtable> GenerateReportTTM(Dictionary<DateTime, Hashtable> report_base)
        {
            report_base = report_base.OrderByDescending(p => p.Key).ToDictionary(p => p.Key, p => p.Value);
            Dictionary<DateTime, Hashtable> report = new Dictionary<DateTime, Hashtable>();
            for (int idx = 0; idx + 4 < report_base.Count; idx++)
            {
                DateTime date = report_base.Keys.ElementAt(idx);
                DateTime annal = DateTime.Today, the_quarter = DateTime.Today;
                switch(date.Month)
                {
                    case 12:
                        report.Add(report_base.Keys.ElementAt(idx), report_base.Values.ElementAt(idx));
                        continue;
                    case 9:
                        annal = report_base.Keys.ElementAt(idx + 3);
                        the_quarter = report_base.Keys.ElementAt(idx + 4);
                        break;
                    case 6:
                        annal = report_base.Keys.ElementAt(idx + 2);
                        the_quarter = report_base.Keys.ElementAt(idx + 4);
                        break;
                    case 3:
                        annal = report_base.Keys.ElementAt(idx + 1);
                        the_quarter = report_base.Keys.ElementAt(idx + 4);
                        break;
                }
                Hashtable set = new Hashtable();
                set.Add("现金流净额", (double)report_base[date]["现金流净额"] + (double)report_base[annal]["现金流净额"] - (double)report_base[the_quarter]["现金流净额"]);
                set.Add("净利润", (double)report_base[date]["净利润"] + (double)report_base[annal]["净利润"] - (double)report_base[the_quarter]["净利润"]);
                set.Add("扣非净利润", (double)report_base[date]["扣非净利润"] + (double)report_base[annal]["扣非净利润"] - (double)report_base[the_quarter]["扣非净利润"]);
                set.Add("期间费用", (double)report_base[date]["期间费用"] + (double)report_base[annal]["期间费用"] - (double)report_base[the_quarter]["期间费用"]);
                set.Add("营业收入", (double)report_base[date]["营业收入"] + (double)report_base[annal]["营业收入"] - (double)report_base[the_quarter]["营业收入"]);
                set.Add("净资产", report_base[date]["净资产"]);
                set.Add("总资产", report_base[date]["总资产"]);
                set.Add("公告日期", report_base[date]["公告日期"]);
                set.Add("code", report_base[date]["code"]);
                report.Add(date, set);
            }
            report = report.OrderBy(p => p.Key).ToDictionary(p => p.Key, p => p.Value);
            return report;
        }

        public void GenerateTrainingRecord(string code)
        {
            string sql = "";
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            DataRowCollection rows = cd.GetRecords("select code from traindb where code='" + code + "'");
            if (rows != null) return;
            Dictionary<DateTime, Hashtable> report_ttm = GetReportTTM(code);
            Dictionary<DateTime, Hashtable> report = new Dictionary<DateTime, Hashtable>();
            ArrayList industry_codes = GetStockIndustry(code);
            for (int i = 0; i < report_ttm.Count; i++)
            {
                DateTime q = report_ttm.Keys.ElementAt(i);
                DateTime begin = DateTime.Parse(report_ttm[q]["公告日期"].ToString());
                DateTime end = DateTime.Today - new TimeSpan(DateTime.Today.Day, 0, 0, 0);
                if (i < report_ttm.Count - 1) end = DateTime.Parse(report_ttm[report_ttm.Keys.ElementAt(i + 1)]["公告日期"].ToString());
                if (begin < new DateTime(2015, 1, 1)) continue;
                int day_count = 0;
                Hashtable rec = new Hashtable();
                Hashtable rec_all = new Hashtable();
                for (DateTime day = begin; day < end; day = day + new TimeSpan(1, 0, 0, 0))
                {
                    if (IsTradeDay(day))
                    {
                        day_count++;
                        Hashtable set = GetFullStockInfo(code, day);
                        if (set.Count > 0)
                        {
                            rec = AddSet(rec, set);
                        }
                        foreach (string c in industry_codes)
                        {
                            Hashtable set_all = GetFullStockInfo(c, day);
                            if (set_all.Count > 0)
                            {
                                rec_all = AddSet(rec_all, set_all);
                            }
                        }
                    }
                }
                if (day_count > 10)
                {
                    if (rec.Count == 10 && rec_all.Count == 10)
                    {
                        sql += GenerateInsertSQL(code, q.ToString("yyyyMM"), rec, rec_all);
                    }
                }
            }
            cd.DoSQL(sql);
        }

        string GenerateInsertSQL(string code, string date, Hashtable rec, Hashtable rec_all)
        {
            Hashtable rate = GenerateRateSet(rec);
            Hashtable rate_all = GenerateRateSet(rec_all);
            Hashtable sub = SubSet(rate, rate_all);
            string sql = "";
            sql += "insert into traindb (code,日期";
            foreach (string item in sub.Keys)
            {
                if (!(item.Contains("日期") || item.Contains("code")))
                {
                    sql += "," + item;
                }
            }
            sql += ") values ('" + code + "','" + date + "'";
            foreach (string item in sub.Keys)
            {
                if (!(item.Contains("日期") || item.Contains("code")))
                {
                    sql += "," + Math.Round(double.Parse(sub[item].ToString()), 6);
                }
            }
            sql += ");";
            return sql;
        }

        public Hashtable AddSet(Hashtable set1, Hashtable set2)
        {
            foreach (string item in set2.Keys)
            {
                if (set1.ContainsKey(item))
                {
                    try
                    {
                        double v1 = double.Parse(set1[item].ToString());
                        double v2 = double.Parse(set2[item].ToString());
                        set1[item] = v1 + v2;
                    }
                    catch { }
                }
                else
                {
                    set1.Add(item, set2[item]);
                }
            }
            return set1;
        }

        public Hashtable SubSet(Hashtable set1, Hashtable set2)
        {
            foreach (string item in set2.Keys)
            {
                if(item=="市盈率")
                {
                    set1.Add("平均市盈率", set2[item]);
                    continue;
                }
                if (set1.ContainsKey(item))
                {
                    try
                    {
                        double v1 = double.Parse(set1[item].ToString());
                        double v2 = double.Parse(set2[item].ToString());
                        set1.Add(item + "对比", v1 - v2);
                    }
                    catch { }
                }
            }
            return set1;
        }

        Dictionary<DateTime, Hashtable> GetReportTTM(string code)
        {
            string sql;
            Dictionary<DateTime, Hashtable> report_ttm = new Dictionary<DateTime, Hashtable>();
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            sql = "select * from 年化财报 where code='" + code + "' order by 截止日期,公告日期";
            DataRowCollection rows = cd.GetRecords(sql);
            if (rows != null)
            {
                foreach (DataRow row in rows)
                {
                    Hashtable set = new Hashtable();
                    set.Add("净利润", double.Parse(row["净利润"].ToString()));
                    set.Add("扣非净利润", double.Parse(row["扣非净利润"].ToString()));
                    set.Add("总资产", double.Parse(row["总资产"].ToString()));
                    set.Add("净资产", double.Parse(row["净资产"].ToString()));
                    set.Add("营业收入", double.Parse(row["营业收入"].ToString()));
                    set.Add("现金流净额", double.Parse(row["现金流净额"].ToString()));
                    set.Add("期间费用", double.Parse(row["期间费用"].ToString()));
                    set.Add("公告日期", DateTime.Parse(row["公告日期"].ToString()));
                    report_ttm.Add(DateTime.Parse(row["截止日期"].ToString()), set);
                }
            }
            return report_ttm;
        }

        public Hashtable GenerateRateSet(Hashtable set)
        {
            Hashtable result = new Hashtable();
            result.Add("业务增长率", Math.Round(((double)set["营业收入"] - (double)set["上期营业收入"]) / (double)set["上期营业收入"], 4));
            result.Add("净资产增长率", Math.Round(((double)set["净资产"] - (double)set["上期净资产"]) / (double)set["上期净资产"], 4));
            result.Add("现金覆盖率", Math.Round((double)set["现金流净额"] / (double)set["净利润"], 4));
            result.Add("净利率", Math.Round((double)set["净利润"] / (double)set["营业收入"], 4));
            result.Add("扣非净资产收益率", Math.Round((double)set["扣非净利润"] / (double)set["净资产"], 4));
            result.Add("管理效率", Math.Round((double)set["期间费用"] / (double)set["营业收入"], 4));
            result.Add("总资产周转率", Math.Round((double)set["营业收入"] / (double)set["总资产"], 4));
            result.Add("资产负债率", Math.Round(((double)set["总资产"] - (double)set["净资产"]) / (double)set["总资产"], 4));
            result.Add("市盈率", Math.Round((double)set["总市值"] / (double)set["扣非净利润"], 4));
            return result;
        }

        public ArrayList GetStockIndustry(string code)
        {
            ArrayList codes = new ArrayList();
            string sql;
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            sql = "select distinct code from stockbase where i_id in (select i_id from stockbase where code='" + code + "' and rownum<2) and code!='" + code + "'";
            DataRowCollection rows = cd.GetRecords(sql);
            if (rows != null)
            {
                foreach(DataRow row in rows)
                {
                    codes.Add(row["code"].ToString());
                }
            }
            return codes;
        }

        public Hashtable GetFullStockInfo(string code,DateTime date)
        {
            Hashtable set = new Hashtable();
            string sql;
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            sql = "select shares,vwap from stockbase where code='" + code + "' and to_char(\"date\",'yyyymmdd')='" + date.ToString("yyyyMMdd") + "'";
            DataRowCollection rows = cd.GetRecords(sql);
            if (rows != null)
            {
                double shares = double.Parse(rows[0]["shares"].ToString());
                double vwap = double.Parse(rows[0]["vwap"].ToString());
                set.Add("总市值", shares * vwap);
                sql = "select * from (select * from 年化财报 where code='" + code + "' and to_date('" + date.ToString("yyyyMMdd") + "','yyyymmdd')>=公告日期 order by 公告日期 desc,截止日期 desc) where rownum<2";
                rows = cd.GetRecords(sql);
                if (rows != null)
                {
                    if (double.Parse(rows[0]["净利润"].ToString()) <= 0) return new Hashtable();                /////净利润为负不统计
                    set.Add("净利润", double.Parse(rows[0]["净利润"].ToString()));
                    set.Add("扣非净利润", double.Parse(rows[0]["扣非净利润"].ToString()));
                    set.Add("总资产", double.Parse(rows[0]["总资产"].ToString()));
                    set.Add("净资产", double.Parse(rows[0]["净资产"].ToString()));
                    set.Add("营业收入", double.Parse(rows[0]["营业收入"].ToString()));
                    set.Add("现金流净额", double.Parse(rows[0]["现金流净额"].ToString()));
                    set.Add("期间费用", double.Parse(rows[0]["期间费用"].ToString()));
                    DateTime expire = DateTime.Parse(rows[0]["截止日期"].ToString());
                    sql = "select * from (select * from 年化财报 where code='" + code + "' and to_date('" + expire.ToString("yyyyMMdd") + "','yyyymmdd')>截止日期 order by 公告日期 desc,截止日期 desc) where rownum<2";
                    rows = cd.GetRecords(sql);
                    if (rows != null)
                    {
                        set.Add("上期净资产", double.Parse(rows[0]["净资产"].ToString()));
                        set.Add("上期营业收入", double.Parse(rows[0]["营业收入"].ToString()));
                    }
                }
            }
            return set;
        }

        public void CleanDatabase()
        {
            CloudDatabase cd = new CloudDatabase(settings, NoTradeDates);
            cd.DoSQL("drop table tmp");
            cd.DoSQL("create table tmp as select distinct * from 财报");
            cd.DoSQL("drop table 财报");
            cd.DoSQL("create table 财报 as select * from tmp order by code,截止日期,公告日期");
            cd.DoSQL("CREATE INDEX \"INDEXCAIBAO\" ON \"财报\" (\"截止日期\",\"CODE\",  \"公告日期\")");
        }
    }
}
