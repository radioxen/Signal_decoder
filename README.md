# Signal Parser
Signal parser is a python pipeline for parsing HEX signals of up to 16 bytes long 
sent from a device with multiple into human readable format dataset.
it requires a dataset in CSV.gzip format, loaded in its input directory and
will write the oarsed signals in output directory.

## build virtualenv with python 3.7+ and install requirments:
```console
git clone https://github.com/radioxen/Signal_decoder.git
virtualenv venv --python=python3.9
source venv/bin/activate
pip install -r requirements.txt
```

## run the script using python,:
The commadn below execute the pipeline with the default executor (Parallel Processing) 
if you want to 
```console
python3 main.py parallel "absolute/path/to/file"
```
*To set the number of parallel worker threads, change the ``NUM_CPUS`` variable in the .env file, default is 8.*

###Other options for executing the pipeline are:

##### linear executor
 ```console
  python3 main.py generator "absolute/path/to/file"
 ```
which uses a async generator object, which execution time is 
comparable to parallel processing with 3-4 CPU cores, depending on the memory, CPU cash and Batch_Size.
##### ray executor
 ```console
  python3 main.py generator "absolute/path/to/file"
 ```
*To change the batch_size in Mb change the ``BATCH_SIZE_IN_Mb`` variable in the .env file., default is 4 Mb*
**Note that batch_size is the size of the file loaded directly in the CPU cash, and not your physical memory, which is maintained efficiently (and generously) assigned by the OS. overloading the CPU cash result in termination of the executor by the OS**

##### linear executor
 ```console
  python3 main.py ray "absolute/path/to/file"
 ```
Implementation of Modin with Ray backend engine for resource management and parallel processing, 
analogous to parallel processing.

####Notes:
If the path to file is not passed, The executors will use the path below by default:
`input/data.csv.gz`

So to leave that param empty, the target file which should be named `data.csv.gzip` 
and already stored in the `input/` directory within the root of the project. 

e.g.
```console
 python3 main.py 
```