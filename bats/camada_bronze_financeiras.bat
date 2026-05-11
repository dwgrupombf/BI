@echo off
python "E:\BI\scripts\Brasilcard\rpa_brasilcard.py"
python "E:\BI\scripts\Brasilcard\refresh_brasilcard_dataflow.py"
python "E:\BI\scripts\Capim\rpa_capim.py"
python "E:\BI\scripts\Capim\refresh_capim_dataflow.py"