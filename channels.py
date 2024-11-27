import json
import os.path
import threading

import requests


def process_result_item(item):
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
    return item


def download_channel_data(
        channelid,
        filename, getAll=True,
        since_date=None,
        until_date=None,
):
    """下载频道数据

    本函数通过循环请求特定频道的所有数据，并将其处理后保存到指定文件中


        :param channelid:
        :param filename:
        :return:
        @param channelid 频道ID，用于构建请求URL
    @param filename 数据保存的文件名
    """
    result = []
    iterator = channel_data_generator(channelid, getAll, since_date, until_date)
    for item in iterator:
        item = process_result_item(item)
        result.append(item)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(result, indent=4, ensure_ascii=False))
        print("文件写入成功:" + filename)


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
            "_pageSize": 40,
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
                    if until_date is None or (
                            until_date is not None and publish_date <= until_date
                    ):
                        yield item

            page += 1
        else:
            break
            # return
    return


def parallel_download(
        channels,
        folder, getAll=True,
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
    threads = []
    for channel in channels:
        if not os.path.exists(folder):
            os.mkdir(folder)

        relpath = (
            (
                os.path.join(
                    folder,
                    channel + "-" + since_date + "-" + until_date + ".json",
                )
            )
            if since_date or until_date
            else (os.path.join(folder, channel + ".json"))
        )
        thread = threading.Thread(
            target=download_channel_data,
            args=(
                channel,
                relpath,
            ), kwargs={"getAll": getAll, "since_date": since_date, "until_date": until_date}
        )
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    current_directory = os.getcwd()
    channels = [
        "febe5cf9074b4ce6a52fd3d34d7a5cba",
        "55dbc14f9bea476bb09743d5f1c8c842",
        "c8318fc200764e38b30116c2d5f72b4b",
        "9ebc4198232e496e8bebf1b1bb1778ef",
    ]

    folder = os.path.join(current_directory, "download")
    parallel_download(
        channels,
        folder, getAll=True
    )
