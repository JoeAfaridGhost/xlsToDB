import os
import re
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import uuid


# 环境变量
load_dotenv()
DB_TYPE = os.getenv("DB_TYPE")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
EXCEL_FILE_PATH1 = os.getenv("EXCEL_FILE_PATH1")
EXCEL_FILE_PATH2 = os.getenv("EXCEL_FILE_PATH2")


# 连接数据库
def get_database_uri():
    return f"{DB_TYPE}+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"



# Contract 表
def import_to_contract_table(engine, data):
    print("开始处理 Contract 表数据...")

    # 清理空格
    data.columns = data.columns.astype(str).str.strip()
    print("清理后的列名：", data.columns.tolist())

    # 映射
    contract_mapping = {
        "合同编号": "contractId",
        "乙方名称": "company",
        "位置": "address",
        "已出租面积": "area",
        "起": "startDate",
        "止": "endDate",
        "租金＋物管费                  （元 / M2·月）": "rentPropertyFee"
    }

    try:
        # 根据映射选择列并重命名
        contract_data = data[list(contract_mapping.keys())].rename(columns=contract_mapping)
        print("映射后的 Contract 数据（前五行）：\n", contract_data.head())
    except KeyError as e:
        raise KeyError(f"Excel 文件中未找到指定的 Contract 列，请检查列名是否正确：{e}")

    # 排除特殊情况的记录
    exclusion_keywords = ["租赁合同解除通知", "换租请示", "解除合同通知", "退租请示"]
    exclusion_pattern = '|'.join(exclusion_keywords)
    contract_data = contract_data[~contract_data['contractId'].str.contains(exclusion_pattern, na=False)]
    print("已排除包含关键字的记录（前五行）：\n", contract_data.head())

    # 去除 contractId 为空
    contract_data = contract_data[contract_data['contractId'].notna()]
    print("已排除 contractId 为空的记录（前五行）：\n", contract_data.head())

    # 去除重复的合同编号，保留第一条记录
    contract_data = contract_data.drop_duplicates(subset=['contractId'])
    print("去重后的 Contract 数据（前五行）：\n", contract_data.head())

    # 处理面积：已出租面积 = 已出租面积 + 已退租面积，空值视为 0
    data["已出租面积"] = data["已出租面积"].fillna(0).astype(float)
    data["已退租面积"] = data["已退租面积"].fillna(0).astype(float)
    contract_data["area"] = data["已出租面积"] + data["已退租面积"]

    # 打印处理后的面积数据
    print("处理后的面积数据（前五行）：\n", contract_data[['area']].head())

    # 转换日期格式，仅保留到日
    try:
        contract_data["startDate"] = pd.to_datetime(contract_data["startDate"], errors="coerce").dt.strftime('%Y-%m-%d')
        contract_data["endDate"] = pd.to_datetime(contract_data["endDate"], errors="coerce").dt.strftime('%Y-%m-%d')
    except Exception as e:
        raise ValueError(f"日期格式转换失败：{e}")

    # 去除 startDate 和 endDate 为空
    contract_data = contract_data[contract_data['startDate'].notna() & contract_data['endDate'].notna()]
    print("已排除 startDate 或 endDate 为空的记录（前五行）：\n", contract_data.head())

    # 租金和物业管理费分离
    try:
        print("租金和物业费列原始数据（前五行）：\n", contract_data['rentPropertyFee'].head())

        # 填充空值并清理多余空格
        contract_data['rentPropertyFee'] = contract_data['rentPropertyFee'].fillna("0+0").astype(str).str.replace(
            r'\s+', '', regex=True)

        # 筛选出有效的格式
        valid_format = contract_data['rentPropertyFee'].str.match(r'^\d+(\.\d+)?\+\d+(\.\d+)?$')
        contract_data.loc[~valid_format, 'rentPropertyFee'] = "0+0"  # 将非法格式替换为默认值

        # 拆分租金和物业费
        contract_data[['rentPrice', 'propertyPrice']] = contract_data['rentPropertyFee'].str.split('+',
                                                                                                   expand=True).astype(
            float).round(2)
    except Exception as e:
        raise ValueError(f"租金和物业费列处理失败：{e}")

    # 删除多余的列
    contract_data = contract_data.drop(columns=["rentPropertyFee"])

    print("处理后的 Contract 数据（前五行）：\n", contract_data.head())

    # 导入
    try:
        contract_data.to_sql('contract', engine, if_exists="append", index=False)
        print("数据成功导入到 Contract 表！")
    except Exception as e:
        raise Exception(f"数据导入失败：{e}")
    return contract_data


# Payment 表数据
def import_to_payment_table(engine, data):
    print("开始处理 Payment 表数据...")

    # 清理空格
    data.columns = data.columns.astype(str).str.strip()
    print("清理后的列名：", data.columns.tolist())

    # 映射
    payment_mapping = {
        "合同编号": "contractId",
        "收款期间起": "startDate",
        "收款期间止": "endDate",
        "租金": "rentFee",
        "物业管理费": "propertyFee",
        "小计": "totalFee",
        "发票金额": "actualPayment",
        "发票日期": "proof"
    }

    try:
        # 根据映射选择列并重命名
        payment_data = data[list(payment_mapping.keys())].rename(columns=payment_mapping)
        print("映射后的 Payment 数据（前五行）：\n", payment_data.head())
    except KeyError as e:
        raise KeyError(f"Excel 文件中未找到指定的 Payment 列，请检查列名是否正确：{e}")

    # 排除合同编号为空
    payment_data = payment_data[payment_data['contractId'].notna()]
    print("已排除 contractId 为空的记录（前五行）：\n", payment_data.head())

    # 将特殊情况的和同编号为上一条记录的合同编号
    special_contract_ids = ["租赁合同解除通知", "换租请示", "解除合同通知", "退租请示"]
    last_valid_contract_id = None  # 保存上一条有效的合同编号

    def replace_special_contract_ids(row):
        nonlocal last_valid_contract_id
        if row["contractId"] in special_contract_ids:
            return last_valid_contract_id  # 用上一条记录的合同编号替换
        last_valid_contract_id = row["contractId"]
        return row["contractId"]

    payment_data["contractId"] = payment_data.apply(replace_special_contract_ids, axis=1)
    print("修正后的 Payment 数据（前五行）：\n", payment_data.head())

    # 去除 startDate 和 endDate 为空的记录
    payment_data = payment_data[payment_data['startDate'].notna() & payment_data['endDate'].notna()]
    print("已排除 startDate 或 endDate 为空的记录（前五行）：\n", payment_data.head())

    # 转换日期格式，仅保留到日
    try:
        payment_data["startDate"] = pd.to_datetime(payment_data["startDate"], errors="coerce").dt.strftime('%Y-%m-%d')
        payment_data["endDate"] = pd.to_datetime(payment_data["endDate"], errors="coerce").dt.strftime('%Y-%m-%d')
    except Exception as e:
        raise ValueError(f"日期格式转换失败：{e}")

    # 填充空值和处理金额
    payment_data["rentFee"] = payment_data["rentFee"].fillna(0).astype(float).round(2)
    payment_data["propertyFee"] = payment_data["propertyFee"].fillna(0).astype(float).round(2)
    payment_data["totalFee"] = payment_data["totalFee"].fillna(0).astype(float).round(2)
    payment_data["actualPayment"] = payment_data["actualPayment"].fillna(0).astype(float).round(2)

    # 生成 paymentId
    payment_data["paymentId"] = [str(uuid.uuid4()) for _ in range(len(payment_data))]
    print("生成 paymentId 后的 Payment 数据（前五行）：\n", payment_data.head())

    # 打印处理后的数据
    print("处理后的 Payment 数据（前五行）：\n", payment_data.head())

    # 导入
    try:
        payment_data.to_sql('payment', engine, if_exists="append", index=False)
        print("数据成功导入到 Payment 表！")
    except Exception as e:
        raise Exception(f"数据导入失败：{e}")





def normalize_rent_address_building(building_name):
    """
    将"X号XX院" -> "XX院"
    例如："1号东南院" -> "东南院"
    """
    match = re.match(r'^(\d+)号(.*)$', building_name)
    if match:
        return match.group(2).strip()  # 提取院名部分
    else:
        return building_name.strip()

def try_int(s):
    try:
        return int(s)
    except:
        return None

def extract_rooms(room_str):
    """
    从类似"201、203室"、"302-305、310室"、"515-1"、"207室"提取所有房间号列表
    """
    room_str = room_str.strip()
    if room_str.endswith('室'):
        room_str = room_str[:-1]
    parts = room_str.split('、')
    all_rooms = []
    for p in parts:
        p = p.strip()
        if '-' in p:
            # 范围展开，如"302-305"
            start, end = p.split('-', 1)
            start, end = start.strip(), end.strip()
            start_num, end_num = try_int(start), try_int(end)
            if start_num is not None and end_num is not None and end_num >= start_num:
                for num in range(start_num, end_num+1):
                    all_rooms.append(str(num))
            else:
                # 范围解析失败，就当普通字符串处理
                all_rooms.append(p)
        else:
            # 没有范围的情况，直接加入
            all_rooms.append(p)
    return all_rooms

def parse_address(address):
    """
    解析复杂的地址字符串，可能包含多院舍信息，用'/'分隔。
    例：
    "东南院（1#）201、203室"
    "成贤院（7#）403室/成贤院（8#）408室"
    "中大院（5#）302-305、310室"
    返回列表[(buildingName, roomNumber), ...]多房间、多院舍时返回多个元组。
    """

    # 如果有'/'表示多个院舍信息
    if '/' in address:
        parts = address.split('/')
        result = []
        for p in parts:
            p = p.strip()
            result.extend(parse_address(p))
        return result

    # 匹配院名+楼栋号格式，如 "东南院（1#）", "南工院（2#)"
    match = re.match(r'^(.*?)（(\d+)#）(.*)$', address)
    if not match:
        # 如果不匹配这种格式，有可能是其他格式，如 "三江院（4#）515-1"
        # 如果不包含（\d+#）结构，可尝试更宽松的匹配
        # 这里假设所有均有(数字#)结构，如果没有，根据实际情况修改
        # 尝试更宽松匹配 "^(.*?)（(\d+)#)(.*)$"，注意有些地址可能是 "南工院（2#)416室"右括号不统一
        alt_match = re.match(r'^(.*?)（(\d+)#\)?(.*)$', address)
        if not alt_match:
            # 无法匹配楼栋院名结构，返回空
            return []
        else:
            buildingName = alt_match.group(1).strip()
            buildingNumber = alt_match.group(2).strip()
            remainder = alt_match.group(3).strip()
    else:
        buildingName = match.group(1).strip()
        buildingNumber = match.group(2).strip()
        remainder = match.group(3).strip()

    # remainder中解析房间号列表，如"201、203室"
    rooms = extract_rooms(remainder)
    # 返回 [(院名,房间号), ...]
    # buildingName这里是"东南院"这样的纯院名，不含"X号"
    # 如需要进一步处理，可在此统一格式
    result = []
    for r in rooms:
        result.append((buildingName, r))
    return result

def import_to_address_table(engine, address_data, contract_data_sorted):
    print("开始处理 Address 表数据...")

    # 映射列
    address_mapping = {
        "楼幢号": "buildingName",
        "房间号": "roomNumber",
        "面积数": "roomArea",
        "租金单价（不含物业费）": "rentPrice"
    }

    # 重命名列
    try:
        address_data = address_data[list(address_mapping.keys())].rename(columns=address_mapping)
        print("映射后的 Address 数据（前五行）：\n", address_data.head())
    except KeyError as e:
        raise KeyError(f"Excel 文件中未找到指定的 Address 列，请检查列名是否正确：{e}")

    # buildingName 为 "X号XX院" 格式，roomNumber为字符串
    address_data['buildingName'] = address_data['buildingName'].astype(str)
    address_data['roomNumber'] = address_data['roomNumber'].astype(str)

    # 对 rent_address 中的 buildingName 格式化："X号XX院" -> "XX院"
    address_data['buildingName_normalized'] = address_data['buildingName'].apply(normalize_rent_address_building)

    # contract_data_sorted中有 contractId, address, endDate 等字段
    # 在此解析 contract_data_sorted 的 address 字段，得到 (buildingName, roomNumber) 列表
    # 需要将 contract_data_sorted 中每条地址可能对应多个房间，多院舍信息时需要展开行
    # 首先对每条记录解析地址
    all_records = []
    for idx, row in contract_data_sorted.iterrows():
        c_id = row['contractId']
        c_end = row['endDate']
        c_address = row['address']
        parsed = parse_address(c_address)  # [(buildingName, roomNumber),...]
        # 对每个解析得到的房间号创建一条记录
        for (bName, rNum) in parsed:
            all_records.append({
                'contractId': c_id,
                'endDate': c_end,
                'buildingName_extracted': bName,
                'roomNumber_extracted': rNum
            })

    parsed_df = pd.DataFrame(all_records)
    if parsed_df.empty:
        # 如果没有解析出任何记录，可能没有匹配地址，直接以空合并
        parsed_df = pd.DataFrame(columns=['contractId', 'endDate', 'buildingName_extracted', 'roomNumber_extracted'])

    # 由于 rent_address 中 buildingName_normalized 是 "XX院"
    # 而 buildingName_extracted 是 "XX院" 格式 (从 parse_address 返回的 buildingName是纯院名，无"X号")
    # 因此两者格式已经统一，可以直接合并
    parsed_df['buildingName_extracted'] = parsed_df['buildingName_extracted'].astype(str)
    parsed_df['roomNumber_extracted'] = parsed_df['roomNumber_extracted'].astype(str)

    merged_data = pd.merge(
        address_data,
        parsed_df[['buildingName_extracted', 'roomNumber_extracted', 'contractId', 'endDate']],
        left_on=['buildingName_normalized', 'roomNumber'],
        right_on=['buildingName_extracted', 'roomNumber_extracted'],
        how='left'
    )

    # 删除不需要的列
    merged_data = merged_data.drop(columns=['buildingName_normalized', 'buildingName_extracted', 'roomNumber_extracted'])

    # 确认最终需要的列
    desired_columns = ['buildingName', 'roomNumber', 'roomArea', 'rentPrice', 'contractId', 'endDate']
    missing_columns = [col for col in desired_columns if col not in merged_data.columns]
    if missing_columns:
        raise KeyError(f"缺少必要的列：{missing_columns}")

    merged_data = merged_data[desired_columns]

    # 打印导入前数据
    print("导入前的数据（前五行）：\n", merged_data.head())

    # 导入数据库
    try:
        merged_data.to_sql('rent_address', engine, if_exists="append", index=False)
        print("数据成功导入到 rent_address 表！")
    except Exception as e:
        raise Exception(f"数据导入失败：{e}")




def main():
    try:
        # 连接数据库
        db_uri = get_database_uri()
        engine1 = create_engine(db_uri)
        engine2 = create_engine(db_uri, echo=False, pool_pre_ping=True)
        print(f"已成功连接到数据库：{db_uri}")

        # 读文件
        if not os.path.exists(EXCEL_FILE_PATH1):
            raise FileNotFoundError(f"Excel 文件未找到：{EXCEL_FILE_PATH1}")
        contract_data = pd.read_excel(EXCEL_FILE_PATH1, sheet_name="东大", header=0)
        print("成功读取第一个 Excel 文件！")
        if not os.path.exists(EXCEL_FILE_PATH2):
            raise FileNotFoundError(f"Excel 文件未找到：{EXCEL_FILE_PATH2}")
        address_data = pd.read_excel(EXCEL_FILE_PATH2, sheet_name=0, header=0)
        print("成功读取第二个 Excel 文件！")

        # 打印 DataFrame 的列名
        # print("DataFrame 的列名：")
        # print(address_data.columns.tolist())

        # 导入 Contract 表
        contract_data_to_address = import_to_contract_table(engine1, contract_data)

        # 导入 Payment 表
        import_to_payment_table(engine1, contract_data)

        #导入 Address 表
        import_to_address_table(engine2, address_data, contract_data_to_address)

        print("数据导入完成！")
    except Exception as e:
        print(f"程序运行出错：{e}")


if __name__ == "__main__":
    main()
