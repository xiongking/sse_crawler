import argparse
import logging
import requests
import json
import os
import time
import random
import re
from urllib.parse import urljoin
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta

def validate_security_code(code):
    """校验股票代码是否为6位数字"""
    code = code.strip()  # 去除前后空格
    if len(code) != 6 or not code.isdigit():
        raise ValueError("股票代码必须为6位数字")
    return code.zfill(6)  # 自动补前导零

def setup_logging(security_code):
    """配置日志记录"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = datetime.now().strftime(f"{log_dir}/%Y-%m-%d.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    logging.info(f"启动爬虫，股票代码: {security_code}")

class SSEPDFCrawler:
    def __init__(self, security_code):
        self.security_code = security_code
        self.download_dir = os.path.join(security_code, "公告")
        self.base_url = "https://query.sse.com.cn/security/stock/queryCompanyBulletinNew.do"
        self.pdf_base_url = "https://static.sse.com.cn"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.sse.com.cn/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
        }
        
        # 创建下载目录
        if not os.path.exists('downloads'):
            os.makedirs('downloads')
            
        # 创建一个持久会话
        self.session = self._create_session()

    def _create_session(self):
        """创建具有重试机制的会话"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def _get_acw_sc_v2(self, response_text):
        """解析并获取acw_sc__v2 cookie"""
        try:
            # 提取arg1值
            arg1_match = re.search(r"var arg1='([^']+)'", response_text)
            if not arg1_match:
                return None
                
            arg1 = arg1_match.group(1)
            
            # 这里需要实现JavaScript中的解密逻辑
            # 由于JavaScript解密逻辑较复杂，这里建议使用execjs库执行原始JavaScript代码
            # 需要先安装 pip install PyExecJS
            import execjs
            
            # 读取并执行JavaScript代码
            js_code = """
            function getAcwScV2(arg1) {
                // 这里放置从网页中提取的JavaScript代码
                // 需要把原始代码中的document.cookie等浏览器相关的代码替换掉
                // 返回解密后的值
                var posList = [15,35,29,24,33,16,1,38,10,9,19,31,40,27,22,23,25,13,6,11,39,18,20,8,14,21,32,26,2,30,7,4,17,5,3,28,34,37,12,36];
                var mask = "3000176000856006061501533003690027800375";
                var outPutList = new Array(posList.length);
                
                // 完全兼容ES5的循环写法
                for(var idx=0; idx<posList.length; idx++){
                    var targetPos = posList[idx] - 1; // 位置索引从0开始
                    if(targetPos < arg1.length) {
                        outPutList[idx] = arg1.charAt(targetPos);
                    }
                }
                
                var arg2 = outPutList.join('');
                var arg3 = '';
                
                for(var i=0; i<arg2.length && i<mask.length; i+=2){
                    var strHex = arg2.substr(i, 2);
                    var maskHex = mask.substr(i, 2);
                    var strVal = parseInt(strHex, 16);
                    var maskVal = parseInt(maskHex, 16);
                    var xorVal = strVal ^ maskVal;
                    var xorHex = xorVal.toString(16);
                    if(xorHex.length === 1) {
                        xorHex = '0' + xorHex;
                    }
                    arg3 += xorHex;
                }
                return arg3.toLowerCase();
            }
            """
            
            ctx = execjs.compile(js_code)
            acw_sc__v2 = ctx.call('getAcwScV2', arg1)
            return acw_sc__v2
            
        except Exception as e:
            print(f"解析acw_sc__v2失败: {str(e)}")
            return None

    def get_page_data(self, page_no):
        """获取指定页码的数据"""
        api_headers = self.headers.copy()
        api_headers.update({
            "Host": "query.sse.com.cn",
            "Referer": "https://www.sse.com.cn/",
        })
        
        params = {
            'jsonCallBack': f'jsonpCallback{random.randint(10000000, 99999999)}',
            'isPagination': 'true',
            'pageHelp.pageSize': '10',
            'pageHelp.cacheSize': '1',
            'pageHelp.pageNo': str(page_no),
            'pageHelp.beginPage': str(page_no),
            'pageHelp.endPage': str(page_no),
            'START_DATE': self.start_date or '',
            'END_DATE': self.end_date or '',
            'SECURITY_CODE': self.security_code,
            'TITLE': '',
            'BULLETIN_TYPE': '',
            '_': str(int(time.time() * 1000))
        }
        
        try:
            response = requests.get(self.base_url, params=params, headers=api_headers, timeout=10)
            # 提取JSON数据
            json_str = response.text.split('(', 1)[1].rstrip(')')
            return json.loads(json_str)
        except Exception as e:
            print(f"获取第{page_no}页数据失败: {str(e)}")
            return None

    def extract_pdf_urls(self, json_data):
        """从JSON数据中提取PDF URL"""
        pdf_urls = []
        bulletin_types = set()  # 新增类型收集
        try:
            # 遍历数据数组
            for item_group in json_data['pageHelp']['data']:
                for item in item_group:
                    if isinstance(item, dict) and 'URL' in item:
                        relative_url = item['URL']
                        full_url = urljoin(self.pdf_base_url, relative_url)
                        bulletin_type = item.get('BULLETIN_TYPE_NAME', '其他公告')
                        bulletin_types.add(bulletin_type)
                        
                        pdf_info = {
                            'url': full_url,
                            'title': item['TITLE'],
                            'date': item['SSEDATE'],
                            'bulletin_type': bulletin_type
                        }
                        pdf_urls.append(pdf_info)
            
        except Exception as e:
            print(f"提取PDF URL失败: {str(e)}")
        return pdf_urls

    def download_pdf(self, pdf_info):
        """下载PDF文件"""
        try:
            filename = f"{pdf_info['date']}_{pdf_info['title']}.pdf"
            filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.'))
            if not os.path.exists(self.download_dir):
                os.makedirs(self.download_dir)
            filepath = os.path.join(self.download_dir, filename)
            
            if os.path.exists(filepath):
                logging.info(f"文件已存在，跳过: {filename}")
                return True

            # 第一次请求PDF
            response = self.session.get(pdf_info['url'], headers=self.headers, timeout=30)
            
            # 检查是否需要处理反爬验证
            if 'acw_sc__v2' in response.text:
                print(f"检测到反爬验证，正在处理...")
                # 获取acw_sc__v2值
                acw_sc__v2 = self._get_acw_sc_v2(response.text)
                if not acw_sc__v2:
                    print("无法获取acw_sc__v2")
                    return False
                    
                # 设置cookie
                self.session.cookies.set('acw_sc__v2', acw_sc__v2, domain='.sse.com.cn')
                
                # 重新请求PDF
                response = self.session.get(pdf_info['url'], headers=self.headers, timeout=30)

            # 验证响应
            if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', '').lower():
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                    
                file_size = os.path.getsize(filepath)
                if file_size < 1024:
                    logging.warning(f"文件大小异常 ({file_size} bytes): {filename}")
                    os.remove(filepath)
                    return False
                    
                logging.info(f"成功下载: {filename} ({file_size/1024:.1f} KB)")
                return True
            else:
                logging.error(f"下载失败 {filename}: 响应类型不正确或状态码错误")
                return False
                
        except Exception as e:
            logging.error(f"下载失败 {pdf_info['url']}: {str(e)}")
            return False

    def crawl(self, start_page=1, end_page=None, start_date=None, end_date=None):
        self.start_date = start_date
        self.end_date = end_date
        """爬取指定页码范围的PDF文件"""
        # 获取第一页来确定总页数
        # 获取总页数并添加错误处理
        try:
            first_page_data = self.get_page_data(1)
            if not first_page_data:
                logging.error("获取总页数失败，请检查网络连接或目标网站状态")
                return
                
            total_pages = first_page_data['pageHelp']['pageCount']
            logging.info(f"成功获取总页数：{total_pages}")
            
        except KeyError as e:
            logging.error(f"解析页面数据失败，网站结构可能已变更：{str(e)}")
            return
        except Exception as e:
            logging.error(f"初始化失败：{str(e)}")
            return
        end_page = end_page or total_pages
        print(f"总页数: {total_pages}")
        
        for page in range(start_page, min(end_page + 1, total_pages + 1)):
            print(f"\n正在处理第 {page}/{total_pages} 页")
            
            # 获取页面数据
            json_data = self.get_page_data(page)
            if not json_data:
                continue
                
            # 提取PDF URL
            pdf_urls = self.extract_pdf_urls(json_data)
            
            # 下载PDF
            for pdf_info in pdf_urls:
                self.download_pdf(pdf_info)
                # 添加随机延时
                time.sleep(random.uniform(1, 3))
            
            # 页面间延时
            time.sleep(random.uniform(2, 4))

def validate_date(date_str):
    """校验日期格式是否为YYYY-MM-DD"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def get_security_code():
    """获取并校验股票代码"""
    while True:
        code = input("请输入6位股票代码：")
        if validate_security_code(code):
            return code
        print("输入错误，请重新输入！")

def select_crawl_mode():
    """选择爬取模式"""
    print("\n请选择爬取模式：")
    print("1. 爬取最近30天公告")
    print("2. 指定页码范围爬取")
    print("3. 指定日期范围爬取")
    while True:
        choice = input("请输入选项(1/2/3)：")
        if choice in ('1', '2', '3'):
            return choice
        print("无效输入，请重新选择！")

def get_page_range():
    """获取页码范围"""
    while True:
        try:
            start = int(input("起始页码："))
            end = int(input("结束页码："))
            if start > 0 and end >= start:
                return start, end
            print("页码输入无效！")
        except ValueError:
            print("请输入数字！")

def get_date_range():
    """获取日期范围"""
    while True:
        start = input("起始日期(YYYY-MM-DD)：")
        end = input("结束日期(YYYY-MM-DD)：")
        if validate_date(start) and validate_date(end):
            return start, end
        print("日期格式错误，请按YYYY-MM-DD格式重新输入！")

if __name__ == '__main__':
    code = get_security_code()
    setup_logging(code)
    
    try:
        crawler = SSEPDFCrawler(code)
        mode = select_crawl_mode()
        
        if mode == '1':
            # 计算最近30天日期
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            crawler.crawl(start_date=start_date, end_date=end_date)
        elif mode == '2':
            start_page, end_page = get_page_range()
            crawler.crawl(start_page=start_page, end_page=end_page)
        elif mode == '3':
            start_date, end_date = get_date_range()
            crawler.crawl(start_date=start_date, end_date=end_date)
            
    except Exception as e:
        logging.error(f"程序运行异常: {str(e)}", exc_info=True)
        raise
