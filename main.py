import argparse
from worker import *


def main():
    argp = argparse.ArgumentParser()
    argp.add_argument("-dir", type=str, help="Directory with files", required=True)
    argp.add_argument("-n", type=int, help="Threads count", default=1)
    argp.add_argument("-db", type=str, help="SQLite db name", default="Density.db")

    args = argp.parse_args()
    logging.info("=== New task at directory: '{}'. ===".format(args.dir))
    # название таблицы не стал добавлять в аргументы, думаю это не принципиально
    try:
        wrk = Worker("density_fraud", args.db, args.dir)
        wrk.do_work(args.n)
    except Exception as e:
        logging.error("Exception thrown: '{0}'. Process stopped.".format(str(e)))
        exit(1)
    else:
        logging.info("All files successfully processed.")

if __name__ == "__main__":
    main()
