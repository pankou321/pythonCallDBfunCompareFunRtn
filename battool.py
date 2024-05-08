import os
import csv
import cx_Oracle
import psycopg2
import threading

def read_functions(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        functions = []
        for line in lines:
            #＃の後はコメントなので、無視する
            func_info = line.split('#')[0].strip()
            if func_info:  
                func_name, args_str = func_info.split(',', 1)
                args = eval(args_str)
                functions.append((func_name.strip(), args))
    return functions


def call_function(func_name, args, flg, output_folder, oracle_conn=None, postgres_conn=None):
    output_file = os.path.join(output_folder, f"{func_name.replace('.', '-')}-{flg}.txt")
    with open(output_file, 'w', encoding='utf-8') as file:
        # フラグパラメータに応じてデータベース接続と実行方法を選択します
        if flg == "oracle":
            cur = oracle_conn.cursor()
            result = cur.callfunc(func_name, cx_Oracle.STRING, args)
            file.write(f"結果: {result}\n")
            return result
        
        elif flg == "postgresql":
            cur = postgres_conn.cursor()
            # パラメータの数に応じて動的に SQL クエリを構築します
            placeholders = ', '.join(['%s' for _ in range(len(args))])
            sql = f"SELECT {func_name}({placeholders})"
            cur.execute(sql, args)
            result = cur.fetchone()[0]
            file.write(f"結果: {result}\n")
            return result
        
        else:
            print("エラー：サポートされていないデータベースタイプです！\n")

# タイムアウト処理関数
def timeout_handler(func_name, args, flg, output_folder, oracle_conn=None, postgres_conn=None):
    print(f"{func_name} の処理はタイムアウトです")
    pass

# Oracle データベースに接続します
oracle_host = "192.168.0.37"
oracle_port = 1521
oracle_sid = "unif"
oracle_user = "system"
oracle_password = "UNIVEAM"
oracle_dsn = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_sid)
oracle_conn = cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn)

# PostgreSQL データベースに接続します
postgresql_dbname = "unif"
postgresql_user = "postgres"
postgresql_password = "postgres"
#postgresql_host = "192.168.0.38"
postgresql_host = "127.0.0.1"
postgresql_port = "5432"
postgresql_conn = psycopg2.connect(dbname=postgresql_dbname, user=postgresql_user, password=postgresql_password, host=postgresql_host, port=postgresql_port)

# 関数とパラメータを読み込みます
functions = read_functions('funs.txt')

# 出力フォルダ
output_folder = 'output'
os.makedirs(output_folder, exist_ok=True)

# CSV ファイルのヘッダー
csv_header = ['関数名', '結果', 'ファイル(./output/)']

# CSV ファイルの書き込み
with open('result.csv', 'w', newline='', encoding='utf-8-sig') as csv_output_file:
    csv_writer = csv.writer(csv_output_file)
    csv_writer.writerow(csv_header)

    # トランザクションを開始します
    oracle_conn.begin()
    postgresql_conn.autocommit = False

    # 関数を呼び出して比較します
    print(f"\n")
    print(f"============================================\n")
    print(f"比較開始...")
    print(f"--------------------------------------------\n")
    try:
        for func_name, args in functions:
            timeout = 60  # タイムアウト時間は60秒
            oracle_thread = threading.Thread(target=call_function, args=(func_name, args, 'oracle', output_folder), kwargs={'oracle_conn': oracle_conn})
            postgres_thread = threading.Thread(target=call_function, args=(func_name, args, 'postgresql', output_folder), kwargs={'postgres_conn': postgresql_conn})

            oracle_thread.start()
            postgres_thread.start()

            oracle_thread.join(timeout)
            postgres_thread.join(timeout)

            if oracle_thread.is_alive():
                timeout_handler(func_name, args, 'oracle', output_folder, oracle_conn=oracle_conn)
                oracle_thread.join()

            if postgres_thread.is_alive():
                timeout_handler(func_name, args, 'postgresql', output_folder, postgres_conn=postgresql_conn)
                postgres_thread.join()

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
    except Exception as e:
        # エラーが発生した場合、トランザクションをロールバックします
        print(f"エラーが発生しました: {e}")

    finally:
        # 比較結果に関係なく、データベースデータを以前の状態に戻します
        oracle_conn.rollback()
        postgresql_conn.rollback()
        # PostgreSQL 接続の autocommit 設定を元に戻します
        postgresql_conn.autocommit = True
        print(f"--------------------------------------------\n")
        print(f"比較完了")
        print(f"--------------------------------------------\n")
        print(f"データベースデータを以前の状態に戻しました")
        print(f"============================================\n")
# ファイルとデータベース接続を閉じます
oracle_conn.close()
postgresql_conn.close()
