import os
import csv
import cx_Oracle
import psycopg2
import concurrent.futures
import re
def read_functions(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        functions = []
        skipped_functions = []
        for line in lines:
            # 忽略以 # 开头的注释行
            if line.strip().startswith('#'):
                func_info = line.strip()
                func_name = func_info.split('#')[1].split(',')[0].strip()  # 提取函数名
                skipped_functions.append(func_info)
                continue
            
            func_info = line.strip()
            if func_info:
                # 检查是否包含逗号
                if ',' in func_info:
                    func_name, args_str = func_info.split(',', 1)
                    if args_str.strip() == '[]':
                        args = []  # 处理没有参数的情况
                    else:
                        args = eval(args_str)
                    functions.append((func_name.strip(), args))
                else:
                    print(f"无效的函数定义：{func_info}")
    return functions, skipped_functions

def call_function(flg, func_name, args, output_folder, oracle_conn=None, postgres_conn=None):
    output_file = os.path.join(output_folder, f"{func_name.replace('.', '-')}-{flg}.txt")
    try:
        with open(output_file, 'w', encoding='utf-8') as file:
            if flg == "oracle":
                print(f"调用函数→{func_name} ( Oracle )")
                cur = oracle_conn.cursor()
                result = cur.callfunc(func_name, cx_Oracle.STRING, args)
                print(f"调用完成→{func_name} ( Oracle )")
            elif flg == "postgresql":
                print(f"调用函数→{func_name} ( PostgreSQL )")
                cur = postgres_conn.cursor()
                placeholders = ', '.join(['%s' for _ in range(len(args))])
                sql = f"SELECT {func_name}({placeholders})"
                cur.execute(sql, args)
                result = cur.fetchone()[0]
                print(f"调用完成→{func_name} ( PostgreSQL )")
            else:
                print("错误：不支持的数据库类型！\n")
                return
            
            file.write(f"结果: {result}\n")
    except Exception as e:
        print(f"函数调用错误： {e}")
        # 回滚事务
        if flg == "oracle" and oracle_conn is not None:
            oracle_conn.rollback()
        elif flg == "postgresql" and postgres_conn is not None:
            postgres_conn.rollback()

# 处理函数调用，忽略超时的调用
def process_function_call(flg, func_info, output_folder, cancelled_funcs, oracle_conn=None, postgres_conn=None):
    func_name, args = func_info
    
    try:
        call_function(flg, func_name, args, output_folder, oracle_conn=oracle_conn, postgres_conn=postgresql_conn)
    except concurrent.futures.TimeoutError as exc:
        print(f"忽略超时的调用: {func_name}")
        cancelled_funcs.append(func_name)
        # 回滚事务
        if flg == "oracle" and oracle_conn is not None:
            oracle_conn.rollback()
        elif flg == "postgresql" and postgres_conn is not None:
            postgres_conn.rollback()
    except Exception as e:
        print(f"错误：{e}")
        # 回滚事务
        if flg == "oracle" and oracle_conn is not None:
            oracle_conn.rollback()
        elif flg == "postgresql" and postgres_conn is not None:
            postgres_conn.rollback()

# Oracle 数据库连接信息
oracle_host = "192.168.0.37"
oracle_port = 1521
oracle_sid = "unif"
oracle_user = "system"
oracle_password = "UNIVEAM"
oracle_dsn = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_sid)
oracle_conn = cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn)

# PostgreSQL 数据库连接信息
postgresql_dbname = "unif"
postgresql_user = "postgres"
postgresql_password = "postgres"
postgresql_host = "127.0.0.1"
postgresql_port = "5432"
postgresql_conn = psycopg2.connect(dbname=postgresql_dbname, user=postgresql_user, password=postgresql_password, host=postgresql_host, port=postgresql_port)
# 读取函数及参数
functions,skipfunctions = read_functions('funs.txt')

# 创建输出文件夹
output_folder = 'output'
os.makedirs(output_folder, exist_ok=True)

# 跟踪取消的函数名的集合
cancelled_funcs = []

# CSV 文件头
csv_header = ['函数名', '结果','oracleリターン','postgresqlリターン', 'スキップ函数', 'スキップ函数エラー', 'Oracle文件(./output/)', 'PostgreSQL文件(./output/)']

# 写入 CSV 文件
with open('result.csv', 'w', newline='', encoding='utf-8-sig') as csv_output_file:
    csv_writer = csv.writer(csv_output_file)
    csv_writer.writerow(csv_header)

    # 开始事务
    if isinstance(oracle_conn, cx_Oracle.Connection):
        oracle_conn.autocommit = False
    if isinstance(postgresql_conn, psycopg2.extensions.connection):
        postgresql_conn.autocommit = False

    # 调用函数并比较结果
    print(f"\n")
    print(f"============================================\n")
    print(f"[1]   调用函数...")
    print(f"--------------------------------------------\n")
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  # 设置最大工作线程数为10
            print(f"[2]   发送任务到线程池.")
            # 发送任务到线程池
            futures = []
            for func_info in functions:
                for flg in ["oracle", "postgresql"]:
                    futures.append(executor.submit(process_function_call, flg, func_info, output_folder, cancelled_funcs, oracle_conn=oracle_conn, postgres_conn=postgresql_conn))
            
            # 等待任务完成，超时为10秒
            print(f"[3] 设置超时为10秒")
            for future in concurrent.futures.as_completed(futures, timeout=10):
                pass
            # 移除已取消的任务
            futures = [future for future in futures if not future.cancelled()]

            # 进行比较
            print(f"--------------------------------------------\n")
            print(f"开始比较")
            print(f"--------------------------------------------\n")
            for func_name, args in functions:
                if func_name in cancelled_funcs:
                    print(f"函数 {func_name} 已被取消，跳过比较。")
                    continue
                
                oracle_file = os.path.join(output_folder, f"{func_name.replace('.', '-')}-oracle.txt")
                postgresql_file = os.path.join(output_folder, f"{func_name.replace('.', '-')}-postgresql.txt")
        
                compare_result = ""
                with open(oracle_file, 'r', encoding='utf-8') as f1, open(postgresql_file, 'r', encoding='utf-8') as f2:
                    lines1 = f1.readlines()
                    lines2 = f2.readlines()

                    if lines1 == lines2:
                        compare_result = "相同"
                    else:
                        compare_result = "不同"

                csv_writer.writerow([func_name, compare_result, lines1, lines2, "", "", oracle_file, postgresql_file])
                print(f"{func_name} 的比较结果：{compare_result}")
            print(f"--------------------------------------------\n")
            print(f"比较完成")
            for skipped_fun in skipfunctions:
                parts = skipped_fun.split('#', 2)
                if len(parts) > 1:
                    func_name = parts[1].strip()
                    args = parts[2].strip() if len(parts) > 2 else ''
                    csv_writer.writerow(["", "", "", "", func_name, args, ""])
            
    except Exception as e:
        print(f"错误：{e}")

    finally:
        # 不论比较结果如何，都将数据库数据回滚到之前的状态
        if isinstance(oracle_conn, cx_Oracle.Connection):
            oracle_conn.rollback()
        if isinstance(postgresql_conn, psycopg2.extensions.connection):
            postgresql_conn.rollback()
        # 结束事务
        if isinstance(oracle_conn, cx_Oracle.Connection):
            oracle_conn.autocommit = True
        if isinstance(postgresql_conn, psycopg2.extensions.connection):
            postgresql_conn.autocommit = True

        print(f"--------------------------------------------\n")
        print(f"数据库数据已回滚到之前的状态")
        print(f"============================================\n")
        # 关闭文件和数据库连接
        oracle_conn.close()
        postgresql_conn.close()

