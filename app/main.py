import streamlit as st
from app.config import config

def main():
    st.set_page_config(page_title="CoE Decision Intelligence", layout="wide")

    st.title("CoE Decision Intelligence System")

    st.write("Environment:", config.APP_ENV)
    st.write("Data Path:", config.DATA_PATH)

if __name__ == "__main__":
    main()