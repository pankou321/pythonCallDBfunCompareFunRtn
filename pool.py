import os
import csv
import cx_Oracle
import psycopg2
import sys
import concurrent.futures

def read_functions(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        functions = []
        for line in lines:
            # 忽略以#开头的注释行
            func_info = line.split('#')[0].strip()
            if func_info:  
                func_name, args_str = func_info.split(',', 1)
                args = eval(args_str)
                functions.append((func_name.strip(), args))
    return functions

def call_function(flg, func_name, args, output_folder, oracle_conn=None, postgres_conn=None):
    output_file = os.path.join(output_folder, f"{func_name.replace('.', '-')}-{flg}.txt")
    try:
        with open(output_file, 'w', encoding='utf-8') as file:
            if flg == "oracle":
                cur = oracle_conn.cursor()
                result = cur.callfunc(func_name, cx_Oracle.STRING, args)
            elif flg == "postgresql":
                cur = postgres_conn.cursor()
                placeholders = ', '.join(['%s' for _ in range(len(args))])
                sql = f"SELECT {func_name}({placeholders})"
                cur.execute(sql, args)
                result = cur.fetchone()[0]
            else:
                print("Error: Unsupported database type!\n")
                return
            
            file.write(f"Result: {result}\n")
    except Exception as e:
        print(f"Function call error: {e}")

# 处理函数调用并忽略超时的调用
def process_function_call(flg, func_info, output_folder, oracle_conn=None, postgres_conn=None):
    func_name, args = func_info
    print(f"Calling function→{func_name} ( {flg} )")

    try:
        call_function(flg, func_name, args, output_folder, oracle_conn=oracle_conn, postgres_conn=postgresql_conn)
    except concurrent.futures.TimeoutError as exc:
        print(f"Ignoring function call timeout for {func_name}")

# Oracle数据库连接信息
oracle_host = "192.168.0.37"
oracle_port = 1521
oracle_sid = "unif"
oracle_user = "system"
oracle_password = "UNIVEAM"
oracle_dsn = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_sid)
oracle_conn = cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn)

# PostgreSQL数据库连接信息
postgresql_dbname = "unif"
postgresql_user = "postgres"
postgresql_password = "postgres"
postgresql_host = "127.0.0.1"
postgresql_port = "5432"
postgresql_conn = psycopg2.connect(dbname=postgresql_dbname, user=postgresql_user, password=postgresql_password, host=postgresql_host, port=postgresql_port)

# 读取函数和参数
functions = read_functions('funs.txt')

# 创建输出文件夹
output_folder = 'output'
os.makedirs(output_folder, exist_ok=True)

# CSV文件的标题
csv_header = ['Function Name', 'Result', 'File(./output/)']

# 写入CSV文件
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
    print(f"Calling functions...")
    print(f"--------------------------------------------\n")
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  # 设置最大并发线程数为10
            # 提交函数调用任务给线程池
            futures = []
            for func_info in functions:
                for flg in ["oracle", "postgresql"]:
                    futures.append(executor.submit(process_function_call, flg, func_info, output_folder, oracle_conn=oracle_conn, postgres_conn=postgresql_conn))
            
            # 使用wait方法等待所有线程结束，并设置超时时间为10秒
            done, not_done = concurrent.futures.wait(futures, timeout=10)
            
            # 检查未完成的任务并处理超时
            for future in not_done:
                future.cancel()
                print(f"Task {future} has timed out and been cancelled.")

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        # 不管比较结果如何，都将数据库数据回滚到先前的状态
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
        print(f"数据库数据已回滚到先前的状态")

# 在所有线程执行结束后进行比较处理
print(f"--------------------------------------------\n")
print(f"比较开始")
print(f"--------------------------------------------\n")
executor.shutdown()  # 移动到这里
for func_name, args in functions:
    oracle_file = os.path.join(output_folder, f"{func_name.replace('.', '-')}-oracle.txt")
    postgresql_file = os.path.join(output_folder, f"{func_name.replace('.', '-')}-postgresql.txt")

    compare_result = ""
    with open(oracle_file, 'r', encoding='utf-8') as f1, open(postgresql_file, 'r', encoding='utf-8') as f2:
        lines1 = f1.readlines()
        lines2 = f2.readlines()

        if lines1 == lines2:
            compare_result = "同じ"
        else:
            compare_result = "異なります"

    csv_writer.writerow([func_name, compare_result, f"{func_name}-oracle.txt, {func_name}-postgresql.txt"])
    print(f"{func_name} の比較結果：{compare_result}")

print(f"--------------------------------------------\n")
print(f"比较结束")
print(f"============================================\n")

# 关闭文件和数据库连接
oracle_conn.close()
postgresql_conn.close()
