import json
import os.path
import queue
import threading
import time
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import requests


def process_result_item(item, originname):
    """
    处理结果项。

    该函数遍历结果项中的域元数据列表，提取每个域的结果数据，并将结果数据的名称和值添加到项字典中。
    完成后，根据条件删除某些键。

    参数:
    item (dict): 包含域元数据列表和内容HTML的字典项。

    返回:
    dict: 处理后的项字典。
    """
    for domainMeta in item["domainMetaList"]:
        for result_data in domainMeta["resultList"]:
            name = result_data["name"]
            value = result_data["value"]
            item[name] = value
    if len((item["domainMetaList"])):
        del item["domainMetaList"]
    if len((item["content"])):
        del item["contentHtml"]
        content = item["content"]
        content = content.replace("&ensp;", "")
        item["content"] = content
    item["部门"] = originname
    return item


def download_channel_data(
    channelid,
    # filename,
    originname,
    getAll=True,
    since_date=None,
    until_date=None,
):
    """下载频道数据

    本函数通过循环请求特定频道的所有数据，并将其处理后保存到指定文件中


        :param until_date:
        :param since_date:
        :param getAll:
        :param channelid:
        :param filename:
        :return:
        @param channelid 频道ID，用于构建请求URL
    # @param filename 数据保存的文件名
    """
    result = []
    iterator = channel_data_generator(channelid, getAll, since_date, until_date)
    for item in iterator:
        item = process_result_item(item, originname)
        result.append(item)
    return result
    # with open(filename, "w", encoding="utf-8") as f:
    #     f.write(json.dumps(result, indent=4, ensure_ascii=False))
    #     print("文件写入成功:" + filename)


def channel_data_generator(channelid, getAll=True, since_date=None, until_date=None):
    """
    生成指定频道内的数据。

    该函数通过调用API来获取指定频道（channelid）内、指定日期范围（since_date至until_date）的数据。

    参数:
    - channelid (str): 频道ID，用于构造API请求URL。
    - since_date (str): 起始日期，获取此日期之后的数据。
    - until_date (str): 结束日期，获取此日期之前的数据。

    返回:
    - generator: 生成器，逐条产出符合条件的数据项。
    """
    page = 1

    while True:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
        apiurl = (
            "http://www.csrc.gov.cn/searchList/"
            + channelid
            + "?_isAgg=true&_isJson=true&_template=index&_rangeTimeGte=&_channelName="
        )
        data_from = {
            "page": page,
            "_pageSize": 50,
        }
        res = requests.get(apiurl, params=data_from, headers=headers)
        # 判断状态码
        if res.status_code != 200:
            raise Exception(
                "请求"
                + res.request.method
                + "-"
                + res.url
                + "-失败:状态码"
                + str(res.status_code)
            )
        print(
            "请求"
            + res.request.method
            + "-"
            + res.url
            + "-成功:状态码"
            + str(res.status_code)
        )
        txt_c = json.loads(res.text)
        if len(txt_c["data"]["results"]):

            for item in txt_c["data"]["results"]:
                if getAll:
                    yield item
                else:
                    publish_date = item["publishedTimeStr"][:10]
                    # print("publish_date:"+publish_date)
                    # print("since_date:" + since_date)
                    # print("until_date:" + until_date)
                    if since_date is not None and publish_date < since_date:
                        # break
                        return
                    if (
                        until_date is not None and publish_date <= until_date
                    ):
                        yield item

            page += 1
        else:
            break
            # return
    return


def transform_columns(df):
    # print("部门："+df["部门"])
    df2 = pd.DataFrame()
    df2["发文日期"] = df["publishedTimeStr"].map(lambda x: x[:10])
    df2["来源"] = df["部门"]
    df2["索引号"] = df["索引号"]
    df2["分类"] = df["channelName"]

    df2["名称"] = df["title"]

    df2["文号"] = df["文号"].map(lambda x: x if len(x) > 0 else "null")
    df2["内容"] = df["content"]
    # 获取当前时间的时间戳
    current_timestamp = time.time()

    # 将时间戳转换为本地时间
    local_time = time.localtime(current_timestamp)

    # 格式化时间
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    df2["爬取时间"] = formatted_time
    return df2


def run_zip_files(folder, getAll, since_date, until_date):
    if getAll:
        datadict: dict[str, list] = {}
        # print(folder)
        for file in os.listdir(folder):
            # print(file)
            if not file.endswith("all.csv"):
                continue
            # print(file)
            filepath = os.path.join(folder, file)
            if os.path.isfile(filepath):
                key = "all"
                if key not in datadict:
                    datadict[key] = []
                datadict[key].append(file)
        for key in datadict:
            value = datadict[key]
            print("名称：" + key, "文件", value)
            zip_path = os.path.join(folder, "SECURITIES_REGULATORY_" + key + ".zip")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_LZMA) as zipf:
                for file in value:
                    file_path = os.path.join(folder, file)
                    zipf.write(file_path, file)
            print("已完成写入" + "压缩文件：" + zip_path)
    else:
        datadict: dict[str, list] = {}
        # print(folder)
        for file in os.listdir(folder):
            # print(file)
            if not file.endswith(".csv") or file.endswith("all.csv"):
                continue
            # print(file)
            filepath = os.path.join(folder, file)
            if os.path.isfile(filepath):
                key = file.split(".")[0][-8:]
                if not (since_date.replace(
                        "-", ""
                ) <= key <= until_date.replace(
                    "-", ""
                )):
                    continue
                if key not in datadict:
                    datadict[key] = []
                datadict[key].append(file)
        if len(datadict) == 0:

            start = datetime.strptime(since_date, '%Y-%m-%d')
            end = datetime.strptime(until_date, '%Y-%m-%d')

            # 使用循环从开始日期到结束日期打印所有日期
            current_date = start
            while current_date <= end:
                # print(current_date.strftime('%Y-%m-%d'))
                key = current_date.strftime('%Y%m%d')
                zip_path = os.path.join(folder, "SECURITIES_REGULATORY_" + key + ".zip")
                zipfile.ZipFile(zip_path, "w", zipfile.ZIP_LZMA)
                print("已完成写入" + "压缩文件：" + zip_path)
                current_date += timedelta(days=1)
            return
        for key in datadict:
            value = datadict[key]
            print("名称：" + key, "文件", value)
            zip_path = os.path.join(folder, "SECURITIES_REGULATORY_" + key + ".zip")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_LZMA) as zipf:
                for file in value:
                    file_path = os.path.join(folder, file)
                    zipf.write(file_path, file)
            print("已完成写入" + "压缩文件：" + zip_path)


def parallel_download(
    channels,
    folder,
    origins,
    getAll=True,
    since_date=None,
    until_date=None,
):
    """
    并行下载频道数据到指定文件夹。

       创建与频道数量相等的线程数，每个线程负责下载一个频道的数据。

       参数:
       channels: 包含所有要下载的频道名称的列表。
       folder: 保存下载的频道数据的文件夹名称。
    """
    qr = queue.Queue()
    if not os.path.exists(folder):
        os.mkdir(folder)
    threads = []
    for index, channel in enumerate(channels):
        # channel = channels[index]
        originname = origins[index]
        # relpath = (
        #     (
        #         os.path.join(
        #             folder,
        #             channel + "-" + since_date + "-" + until_date + ".json",
        #         )
        #     )
        #     if since_date or until_date
        #     else (os.path.join(folder, channel + ".json"))
        # )
        thread = threading.Thread(
            target=lambda q, channel2, origin, **ka: q.put(
                download_channel_data(channel2, origin, **ka)
            ),
            args=(
                qr,
                channel,
                originname,
                # relpath,
            ),
            kwargs={
                "getAll": getAll,
                "since_date": since_date,
                "until_date": until_date,
            },
        )
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    results = []
    while not qr.empty():
        results.append(qr.get())

    # print(results)
    if getAll:
        outputdatamap = {}
        for result in results:
            for item in result:
                key = (
                        get_channel_type(item)
                        + "_"
                        + "all"
                )
                if key not in outputdatamap:
                    outputdatamap[key] = []
                outputdatamap[key].append(item)
        for key in outputdatamap:
            outputdata = outputdatamap[key]
            filename = os.path.join(folder, key + ".csv")
            df = pd.DataFrame(outputdata)

            df = transform_columns(df)
            df.to_csv(filename, index=False, sep="|", encoding="utf-8", lineterminator="\n")
            print("文件写入成功:" + filename)
    else:
        outputdatamap = {}
        for result in results:
            for item in result:
                key = (
                        get_channel_type(item)
                        + "_"
                        + item["publishedTimeStr"][:10].replace("-", "")
                )
                if key not in outputdatamap:
                    outputdatamap[key] = []
                outputdatamap[key].append(item)
        for key in outputdatamap:
            outputdata = outputdatamap[key]
            filename = os.path.join(folder, key + ".csv")
            df = pd.DataFrame(outputdata)

            df = transform_columns(df)
            df.to_csv(filename, index=False, sep="|", encoding="utf-8", lineterminator="\n")
            print("文件写入成功:" + filename)
    run_zip_files(folder, getAll, since_date, until_date)


def get_channel_type(item):
    """
    根据item中的channelName属性值，决定返回的字符串类型。

    参数:
    item: 包含channelName属性的字典对象，用于判断返回值。

    返回:
    如果item的channelName属性为"行政监管措施"，则返回"Regulatory_Measures"；
    如果item的channelName属性为"行政处罚决定"，则返回"Penalty_Decisions"；
    否则，返回item的channelName属性值。
    """
    return (
        "Regulatory_Measures"
        if item["channelName"] == "行政监管措施"
        else (
            "Penalty_Decisions"
            if item["channelName"] == "行政处罚决定"
            else item["channelName"]
        )
    )


channels = [
    "febe5cf9074b4ce6a52fd3d34d7a5cba",
    "55dbc14f9bea476bb09743d5f1c8c842",
    "c8318fc200764e38b30116c2d5f72b4b",
    "9ebc4198232e496e8bebf1b1bb1778ef",
]
origins = ["重庆局", "重庆局", "上海局", "上海局"]


def task_download_all():
    current_directory = os.getcwd()

    folder = os.path.join(current_directory, "download")
    parallel_download(channels, folder, origins, getAll=True)


if __name__ == "__main__":
    task_download_all()
