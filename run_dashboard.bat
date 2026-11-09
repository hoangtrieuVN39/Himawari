@echo off
echo Dang khoi dong Streamlit Weather Decision Dashboard...
call .venv\Scripts\activate.bat
streamlit run src/dashboard/app.py
pause
