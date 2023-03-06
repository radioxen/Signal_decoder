# Signal Parser
Signal parser is a python pipeline for parsing HEX signals of up to 16 bytes long 
sent from a device with multiple into human readable format dataset.
it requires a dataset in CSV.gzip format, loaded in its input directory and
will write the oarsed signals in output directory.

## build virtualenv with python 3.7+ and install requirments:
```console
git clone https://github.com/Volta-Charging/data_eng_scripts.git
virtualenv venv --python=python3.7
source venv/bin/activate
pip install -r requirements.txt
```

## run your desired script using python:
e.g.:
```console
python Placer_visitation_data_for_all_Volta_stations.py
```
