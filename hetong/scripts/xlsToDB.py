import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import uuid  # paymentId

# 环境变量
load_dotenv()
DB_TYPE = os.getenv("DB_TYPE", "mysql")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "test_database")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "root")
EXCEL_FILE_PATH = os.getenv("EXCEL_FILE_PATH", "data/conDetail.xls")


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


def main():
    try:
        # 连接数据库
        db_uri = get_database_uri()
        engine = create_engine(db_uri)
        print(f"已成功连接到数据库：{db_uri}")

        # 读文件
        if not os.path.exists(EXCEL_FILE_PATH):
            raise FileNotFoundError(f"Excel 文件未找到：{EXCEL_FILE_PATH}")
        data = pd.read_excel(EXCEL_FILE_PATH, sheet_name="东大", header=0)
        print("成功读取 Excel 文件！")

        # 导入 Contract 表
        import_to_contract_table(engine, data)

        # 导入 Payment 表
        import_to_payment_table(engine, data)

        print("数据导入完成！")
    except Exception as e:
        print(f"程序运行出错：{e}")


if __name__ == "__main__":
    main()
