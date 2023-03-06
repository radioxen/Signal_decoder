
# def mp_parquet_writer(output_path_2 : str = default_output_file_path,
#                       df : pd.DataFrame = None, append=True):
#
#     if append:
#         df.to_parquet(output_path_2, engine="fastparquet", object_encoding='utf8', append=append, index=False)
#
#     else:
#         df = pd.DataFrame([],columns=final_columns)




# --------- main prev ---------------
import logging
from src.executors.single_thread_executor import *
from src.executors.multiprocess_executor import *

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def choose_executor(smoke_test: int = 0):

    if smoke_test in [1, 2, 3, 4]:
        return smoke_test, default_input_file_path, default_output_file_path

    input_path, output_path = default_input_file_path, default_output_file_path

    print(
        """
            Please choose the number that corresponds to your executor of choice:
            - signle threaded executor with generator : enter 1
            - signle threaded executor with generator : enter 2
            - signle threaded executor with generator : enter 3
            - signle threaded executor with generator : enter 4
            
            To cancel, press ctl + c or enter exit"""
    )

    user_choice = eval(input("enter your choice : "))

    if user_choice not in [1, 2, 3, 4]:
        choose_executor()

    return user_choice, input_path, output_path


if __name__ == "__main__":
    df = []
    # cProfile.run('single_executor()')
    # choice = choose_executor(1)
    # user_choice, input_path, output_path = choice
    # if user_choice == 1:
    # print("SINGLE EXECUTOR TIME")
    # single_executor()
    # elif user_choice == 2:
    # print("PARALLEL EXECUTORs TIME")
    # mp_pool_executor()
    # elif user_choice == 3:
    #     pass
    # elif user_choice == 4:
    #     pass
    # else:
    #     print("""Something went wrong,
    #     please report the log file to radioxen@gmail.com along with your inputs""")
    #     exit()
