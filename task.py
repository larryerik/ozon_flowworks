import pandas as pd
import requests
import time
import io
import zipfile
from datetime import datetime, timedelta,timezone
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
from typing import List, Dict, Optional, Tuple
from functools import partial
import logging
from zoneinfo import ZoneInfo

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_token_valid(response: requests.Response) -> bool:
    """检查token是否有效"""
    if response.status_code == 403:
        try:
            error_data = response.json()
            if error_data.get('error') == "Ошибка авторизации, ресурс недоступен":
                return False
        except:
            pass
    return True

@task(retries=3, retry_delay_seconds=60)
def get_token(client_id: str, client_secret: str) -> str:
    """获取访问令牌"""
    logger = get_run_logger()
    logger.info(f"开始获取token，client_id: {client_id[:10]}...")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    api_url = "https://api-performance.ozon.ru:443/api/client/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    try:
        response = requests.post(api_url, headers=headers, json=payload)
        response_json = response.json()
        if 'access_token' in response_json:
            logger.info("成功获取token")
            return response_json['access_token']
        logger.error(f"获取token失败: {response.text}")
        raise Exception("获取token失败")
    except Exception as e:
        logger.error(f"获取token时发生异常: {str(e)}")
        raise

def make_request_with_retry(
    method: str,
    url: str,
    headers: Dict,
    client_id: str,
    client_secret: str,
    **kwargs
) -> Tuple[requests.Response, str]:
    """发送请求并在token过期时自动重试"""
    logger = get_run_logger()
    access_token = headers.get('Authorization', '').replace('Bearer ', '')
    
    response = requests.request(method, url, headers=headers, **kwargs)
    
    if not check_token_valid(response):
        logger.info("Token已过期，重新获取token")
        new_token = get_token(client_id, client_secret)
        headers['Authorization'] = f"Bearer {new_token}"
        response = requests.request(method, url, headers=headers, **kwargs)
        return response, new_token
    
    return response, access_token

@task(retries=3, retry_delay_seconds=600)
def search_campaign_by_sku(from_date: str, to_date: str, access_token: str, client_id: str, client_secret: str) -> str:
    """请求搜索广告数据"""
    logger = get_run_logger()
    logger.info(f"开始请求搜索广告数据，日期范围: {from_date} 到 {to_date}")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    url = "https://api-performance.ozon.ru:443/api/client/statistic/products/generate"
    json_data = {
        "from": f"{from_date}T00:00:00Z",
        "to": f"{to_date}T00:00:00Z"
    }
    try:
        response, new_token = make_request_with_retry('POST', url, headers, client_id, client_secret, json=json_data)
        if response.status_code == 200:
            uuid = response.json()['UUID']
            logger.info(f"成功获取搜索广告UUID: {uuid}")
            return uuid, new_token
        logger.error(f"搜索广告请求失败: {response.status_code} {response.text}")
        raise Exception("搜索广告请求失败")
    except Exception as e:
        logger.error(f"请求搜索广告数据时发生异常: {str(e)}")
        raise

@task(retries=3, retry_delay_seconds=60)
def get_campaign_list(access_token: str, client_id: str, client_secret: str) -> tuple[List[str], pd.DataFrame, str]:
    """获取广告列表"""
    logger = get_run_logger()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    url = "https://api-performance.ozon.ru:443/api/client/campaign"
    params = {
        "advObjectType": 'SKU',
    }
    try:
        response, new_token = make_request_with_retry('GET', url, headers, client_id, client_secret, params=params)
        if response.status_code == 200:
            response_json = response.json()
            df = pd.DataFrame(response_json['list'])
            campaign_list = df['id'].to_list()
            return campaign_list, df, new_token
        logger.error(f"获取广告列表失败: {response.status_code} {response.text}")
        raise Exception("获取广告列表失败")
    except Exception as e:
        logger.error(f"获取广告列表时发生异常: {str(e)}")
        raise

@task(retries=5, retry_delay_seconds=900)
def gen_statistics_report(from_date: str, to_date: str, access_token: str, campaign_ids: List[str], client_id: str, client_secret: str) -> Tuple[str, str]:
    """生成模板报告"""
    logger = get_run_logger()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    url = "https://api-performance.ozon.ru:443/api/client/statistics"
    json_data = {
        "campaigns": campaign_ids,
        "dateFrom": str(from_date),
        "dateTo": str(to_date),
        "groupBy": "DATE"
    }
    try:
        response, new_token = make_request_with_retry('POST', url, headers, client_id, client_secret, json=json_data)
        if response.status_code == 200:
            return response.json()['UUID'], new_token
        logger.error(f"生成模板报告失败: {response.status_code} {response.text}")
        raise Exception("生成模板报告失败")
    except Exception as e:
        logger.error(f"生成模板报告时发生异常: {str(e)}")
        raise

@task(retries=8, retry_delay_seconds=300)
def get_report_data(uuid: str, access_token: str, client_id: str, client_secret: str) -> Tuple[str, str]:
    """获取报告数据链接"""
    logger = get_run_logger()
    url = f"https://api-performance.ozon.ru:443/api/client/statistics/{uuid}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    try:
        response, new_token = make_request_with_retry('GET', url, headers, client_id, client_secret)
        if response.status_code == 200:
            return 'https://api-performance.ozon.ru:443' + response.json()['link'], new_token
        logger.error(f"获取报告数据失败: {response.status_code} {response.text}")
        raise Exception("获取报告数据失败")
    except Exception as e:
        logger.error(f"获取报告数据时发生异常: {str(e)}")
        raise

@task
def process_search_data(csv_link: str, access_token: str, date: str, store_name: str, client_id: str, client_secret: str) -> pd.DataFrame:
    """处理搜索数据"""
    logger = get_run_logger()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    try:
        response, new_token = make_request_with_retry('GET', csv_link, headers, client_id, client_secret)
        if response.status_code == 200:
            df = pd.read_csv(io.StringIO(response.text), sep=';', decimal=',', header=1)
            df['日期'] = date
            df['店铺'] = store_name
            df.rename(columns={
                'Артикул': '商品编号',
                'Наименование': '商品名称',
                'Категория': '类别',
                'Продвижение': '促销',
                'Цена товара, ₽': '商品价格, ₽',
                'Ставка, %': '费率, %',
                'Ставка, ₽': '费率, ₽',
                'Продажи из кампаний за клики, ₽': '点击广告带来的销售额, ₽',
                'Заказы из кампаний за клики, шт.': '点击广告带来的订单数, 件',
                'Расход из кампаний за клики, ₽': '点击广告的支出, ₽',
                'Расход ("Оплата за заказ"), ₽': '支出 ("按订单支付"), ₽',
                'Продажи ("Оплата за заказ"), ₽': '销售额 ("按订单支付"), ₽',
                'Заказы ("Оплата за заказ"), шт.': '订单数 ("按订单支付"), 件',
                'ДРР ("Оплата за заказ"), %': '广告支出回报率 ("按订单支付"), %',
                'Показы ("Оплата за заказ"), шт.': '展示次数 ("按订单支付"), 次',
                'Клики ("Оплата за заказ"), шт.': '点击次数 ("按订单支付"), 次',
                'CTR ("Оплата за заказ"), %': '点击率 ("按订单支付"), %',
                'В корзину ("Оплата за заказ"), шт.': '加入购物车 ("按订单支付"), 件',
                'Последнее изменение': '最后修改时间'
            }, inplace=True)
            return df
        logger.error(f"处理搜索数据失败: {response.status_code} {response.text}")
        raise Exception("处理搜索数据失败")
    except Exception as e:
        logger.error(f"处理搜索数据时发生异常: {str(e)}")
        raise

@task
def process_campaign_data(csv_link: str, access_token: str, campaign_df: pd.DataFrame, store_name: str, client_id: str, client_secret: str) -> pd.DataFrame:
    """处理广告数据"""
    logger = get_run_logger()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    try:
        response, new_token = make_request_with_retry('GET', csv_link, headers, client_id, client_secret)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '').lower()

            if 'zip' in content_type:
                
                zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                df_all = pd.DataFrame()
                for file_name in zip_file.namelist():
                    with zip_file.open(file_name) as file:
                        df = pd.read_csv(file, sep=';', decimal=',', header=1, skipfooter=1, engine='python')
                        df['广告ID'] = file_name.split('_')[0]
                        df_all = pd.concat([df_all, df], ignore_index=True)
            elif 'csv' in content_type:
                df_all = pd.read_csv(io.BytesIO(response.content), sep=';', decimal=',', header=1, skipfooter=1, engine='python')
                df_all['广告ID'] = response.headers.get('Content-Disposition').split('"')[-2].split('_')[0]

            df_all = df_all.merge(campaign_df, left_on='广告ID', right_on='id', how='left')
            df_all['店铺'] = store_name
            df_all.rename(columns={
                'День': '日期',
                'Название товара': '商品名称',
                'Цена товара, ₽': '商品价格',
                'Показы': '展现',
                'Клики': '点击',
                'CTR (%)': 'CTR',
                'В корзину': '加入购物车',
                'Ср. цена клика, ₽': '点击平均价格',
                'Расход, ₽, с НДС': '总花费',
                'Заказы': '订单',
                'Выручка, ₽': '营业额',
                'Заказы модели': '模型订单',
                'Выручка с заказов модели, ₽': '模型营业额',
                'ДРР, %': 'DRR, %',
                'Дата добавления': '添加日期',
            }, inplace=True)
            return df_all
        logger.error(f"处理广告数据失败: {response.status_code} {response.text}")
        raise Exception("处理广告数据失败")
    except Exception as e:
        logger.error(f"处理广告数据时发生异常: {str(e)}")
        raise

@task
def send_to_jiandaoyun(data: pd.DataFrame, webhook_url: str):
    """发送数据到简道云"""
    logger = get_run_logger()
    data_json = data.to_dict(orient='records')
    response = requests.post(webhook_url, json=data_json)
    if response.status_code != 200:
        logger.error(f"发送数据到简道云失败: {response.status_code} {response.text}")
        raise Exception("发送数据到简道云失败")

@flow(name="Ozon广告数据采集")
def ozon_ad_data_collection(store_name: str, client_id: str, client_secret: str):
    """主流程"""
    logger = get_run_logger()
    logger.info(f"开始执行广告数据采集任务，店铺: {store_name}")
    
    try:
        # 获取昨天的日期
        moscow_tz = ZoneInfo('Europe/Moscow')
        yesterday = (datetime.now(moscow_tz) - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"处理日期: {yesterday}")
        
        # 获取token
        access_token = get_token(client_id, client_secret)
        
        # 获取搜索广告数据
        search_uuid, access_token = search_campaign_by_sku(yesterday, yesterday, access_token, client_id, client_secret)
        
        # 获取广告列表
        campaign_list, campaign_df, access_token = get_campaign_list(access_token, client_id, client_secret)
        logger.info(f"成功获取广告列表，共{len(campaign_list)}个广告")
        
        # 按10个广告id分组处理
        report_data_list = []
        for i in range(0, len(campaign_list), 10):
            group = campaign_list[i:i+10]
            logger.info(f"处理广告组 {i//10 + 1}/{(len(campaign_list)-1)//10 + 1}, 广告ID: {group}")
            report_uuid, access_token = gen_statistics_report(yesterday, yesterday, access_token, group, client_id, client_secret)
            report_data_list.append((report_uuid))
            
        
        # 下载搜索广告数据
        search_data_link, access_token = get_report_data(search_uuid, access_token, client_id, client_secret)
        search_df = process_search_data(search_data_link, access_token, yesterday, store_name, client_id, client_secret)
        logger.info(f"成功下载搜索广告数据，数据量: {len(search_df)}行")
        
        # 发送搜索数据到简道云
        send_to_jiandaoyun(
            search_df,
            'https://api.jiandaoyun.com/api/v1/automation/tenant/68199539b3e75878dc16b8c9/hooks/681c90962f9d682f23b8b79e17851386d6c539ed758742ed'
        )
        logger.info("成功发送搜索数据到简道云")
        
        # 下载模板广告数据
        all_campaign_data = pd.DataFrame()
        for uuid in report_data_list:
            report_data_link, access_token = get_report_data(uuid, access_token, client_id, client_secret)
            campaign_data = process_campaign_data(report_data_link, access_token, campaign_df, store_name, client_id, client_secret)
            all_campaign_data = pd.concat([all_campaign_data, campaign_data], ignore_index=True)
            logger.info(f"成功处理模板广告数据，当前总数据量: {len(all_campaign_data)}行")
            time.sleep(10)
        
        # 发送广告数据到简道云
        send_to_jiandaoyun(
            all_campaign_data,
            'https://api.jiandaoyun.com/api/v1/automation/tenant/68199539b3e75878dc16b8c9/hooks/681c90962f9d682f1e34122017851386d6c5697ea8f0bbb1'
        )
        logger.info("成功发送模板广告数据到简道云")
        
    except Exception as e:
        logger.error(f"执行过程中发生错误: {str(e)}")
        raise

if __name__ == "__main__":
    STORE_NAME = "个人舒适"
    client_id = "27647664-1715061218798@advertising.performance.ozon.ru"
    client_secret = "EMWQQP--bsaqW6u2t6Anct_Iox7lILlS0wdBsf1BC4tW5qt4hj-WZn1mdmkSYGcJxqsFwdbOLCsqCfxUGA"
    ozon_ad_data_collection(STORE_NAME, client_id, client_secret)
