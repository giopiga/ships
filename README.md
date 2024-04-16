# Ships
The purpose of this project is to analyse two datasets containing the positions of different ships and calculate the distance travelled.
The results are saved in text files produced as output.


## Requirements
Requirements are listed in requirements.txt file. Please run:
```console
pip install -r requirements.txt
```

## Run 
If source csv files are in folder <your/input/folder>, and the output folder is <your/output/folder>, run:
```console
python3 main.py --data <your/input/folder> --output <your/output/folder>
```
Note that `--data` argument is required, while `--output` is `.` by default. 

The main script will create, in the output folder, for both the datasets:
- a text file with the information about the number of unique ships;
- a text file with the information about the total lengths.

Note that the number of ships will be calculated both if all positions are counted and if invalid positions are eliminated.

## Code and settings
The name of the two csv files are in `settings.yaml`.

The distance calculation function is implemented in `utils.py` module.
