import configparser
import psycopg2


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    query = "SELECT * FROM sys_load_error_detail"
    cur.execute(query)
    results=cur.fetchall()
    print(results)
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()