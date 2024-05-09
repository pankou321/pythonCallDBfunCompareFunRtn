import os
import csv
import cx_Oracle
import psycopg2
import concurrent.futures

def read_functions(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        functions = []
        for line in lines:
            # #で始まるコメント行を無視する
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
                print("エラー：サポートされていないデータベースタイプです！\n")
                return
            
            file.write(f"結果: {result}\n")
    except Exception as e:
        print(f"関数呼び出しエラー： {e}")

# 関数呼び出しを処理し、タイムアウトした呼び出しを無視する
def process_function_call(flg, func_info, output_folder, oracle_conn=None, postgres_conn=None):
    func_name, args = func_info
    print(f"関数の呼び出し→{func_name} ( {flg} )")

    try:
        call_function(flg, func_name, args, output_folder, oracle_conn=oracle_conn, postgres_conn=postgresql_conn)
    except concurrent.futures.TimeoutError as exc:
        print(f"タイムアウトした呼び出しを無視する: {func_name}")
    except Exception as e:
        print(f"エラーが発生しました：{e}")

# Oracleデータベースの接続情報
oracle_host = "192.168.0.37"
oracle_port = 1521
oracle_sid = "unif"
oracle_user = "system"
oracle_password = "UNIVEAM"
oracle_dsn = cx_Oracle.makedsn(oracle_host, oracle_port, oracle_sid)
oracle_conn = cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn)

# PostgreSQLデータベースの接続情報
postgresql_dbname = "unif"
postgresql_user = "postgres"
postgresql_password = "postgres"
postgresql_host = "127.0.0.1"
postgresql_port = "5432"
postgresql_conn = psycopg2.connect(dbname=postgresql_dbname, user=postgresql_user, password=postgresql_password, host=postgresql_host, port=postgresql_port)

# 関数と引数を読み込む
functions = read_functions('funs.txt')

# 出力フォルダを作成する
output_folder = 'output'
os.makedirs(output_folder, exist_ok=True)

# CSVファイルのヘッダー
csv_header = ['関数名', '結果', 'ファイル(./output/)']

# CSVファイルに書き込む
with open('result.csv', 'w', newline='', encoding='utf-8-sig') as csv_output_file:
    csv_writer = csv.writer(csv_output_file)
    csv_writer.writerow(csv_header)

    # トランザクションを開始する
    if isinstance(oracle_conn, cx_Oracle.Connection):
        oracle_conn.autocommit = False
    if isinstance(postgresql_conn, psycopg2.extensions.connection):
        postgresql_conn.autocommit = False

    # 関数を呼び出して結果を比較する
    print(f"\n")
    print(f"============================================\n")
    print(f"関数の呼び出し...")
    print(f"--------------------------------------------\n")
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  # 最大ワーカー数を10に設定
            # スレッドプールに関数呼び出しのタスクを送信する
            futures = []
            for func_info in functions:
                for flg in ["oracle", "postgresql"]:
                    futures.append(executor.submit(process_function_call, flg, func_info, output_folder, oracle_conn=oracle_conn, postgres_conn=postgresql_conn))
            
            # 待機しているタスクが完了するまで待ち、タイムアウトは10秒
            done, not_done = concurrent.futures.wait(futures, timeout=10)
            
            # 完了していないタスクをチェックして、タイムアウトした場合はキャンセルする
            for future in not_done:
                future.cancel()
                print(f"タスク {future} がタイムアウトしてキャンセルされました。")
            
            # キャンセルされたタスクを除去する
            futures = [future for future in futures if not future.cancelled()]
            print(f"タスクキューを整理する.")

            # すべてのスレッドが完了した後に比較処理を行う
            print(f"--------------------------------------------\n")
            print(f"比較を開始する")
            print(f"--------------------------------------------\n")
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
    except Exception as e:
        print(f"エラーが発生しました：{e}")

    finally:
        # 比較結果に関係なく、データベースのデータを以前の状態にロールバックする
        if isinstance(oracle_conn, cx_Oracle.Connection):
            oracle_conn.rollback()
        if isinstance(postgresql_conn, psycopg2.extensions.connection):
            postgresql_conn.rollback()
        # トランザクションを終了する
        if isinstance(oracle_conn, cx_Oracle.Connection):
            oracle_conn.autocommit = True
        if isinstance(postgresql_conn, psycopg2.extensions.connection):
            postgresql_conn.autocommit = True
        print(f"--------------------------------------------\n")
        print(f"比較終了")
        print(f"--------------------------------------------\n")
        print(f"データベースのデータが以前の状態にロールバックされました")
        print(f"============================================\n")
        # ファイルとデータベースの接続を閉じる
        oracle_conn.close()
        postgresql_conn.close()
