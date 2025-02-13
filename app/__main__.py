from app.processor.duty import Processor
import argparse
import time
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Application requires 2 parameters from CMD"
    )
    parser.add_argument("--filename", required=True, type=str, help="Filename, without the path, to load. Needs to be a valid JSON")
    parser.add_argument("--operations", required=False, type=list[str],
        help="""Operations to be executed. If none provided, all operations will be performed.
        Operations available are:\n a -> all\n b -> Breaks\n s -> Start and end time\n n -> Start and end time with stop names"""
    )
    args = parser.parse_args()

    file = args.filename
    operations = args.operations

    print("Application is starting...")
    start_time = time.time()
    processor = Processor(file, auto_operations=operations)
    processor.start()
    print("Application finished in {} seconds".format(time.time() - start_time))