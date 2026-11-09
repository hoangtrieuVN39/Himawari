@echo off
echo Dang khoi dong Streamlit Weather Decision Dashboard...
call .venv\Scripts\activate.bat
set PYTHONPATH=%~dp0
streamlit run src/dashboard/app.py
pause
