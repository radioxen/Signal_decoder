import sys
from src.executors.modin_ray_executor import *
from src.executors.single_thread_executor import *
from src.executors.multiprocess_executor import *


if __name__ == "__main__":

    if not os.path.exists("output"):
        os.mkdir("output")

    if len(sys.argv) > 1:

        if sys.argv[1] == "ray":
            print("Processsing with Modin/Ray EXECUTOR")
            if len(sys.argv) > 2:
                modin_main(input_path)
            else:
                modin_main()

        elif sys.argv[1] == "generator":
            print("Processsing with SINGLE EXECUTOR")
            if len(sys.argv) > 2:
                single_executor(input_path)
            else:
                single_executor(input_path)

        elif sys.argv[1] == "parallel":
            print("Processsing with Parallel EXECUTOR")
            if len(sys.argv) > 2:
                mp_pool_executor(input_path)
            else:
                mp_pool_executor()


        elif sys.argv[1] == "parallel_at_once":
            print("Processsing with Parallel EXECUTOR, without mediatory dataframe")
            if len(sys.argv) > 2:
                mp_pool_executor_at_once(input_path)
            else:
                mp_pool_executor_at_once()

    else:
        print(
            """Using default executor, Parallel executor,
              To run the process with other executors, use the below command
              for single thread linear executor run this command: python3 main.py parallel "absoulte/path/to/file or None"
              for single thread linear executor run this command: python3 main.py generator "absoulte/path/to/file or None"
              for Modin with Ray backend executor run this command: python3 main.py ray "absoulte/path/to/file or None" """
        )

        print("Processsing with Parallel EXECUTOR")
        df = mp_pool_executor()
