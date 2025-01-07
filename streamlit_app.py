import snowflake.snowpark
import streamlit as st
import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.cortex import Complete

session = Session.builder.config("connection_name", "default").create()

prompt: str = st.chat_input("Enter a prompt here")

USER = "user"
ASSISTANT = "assistant"

if prompt:
    st.chat_message(USER).write(prompt)

    response = Complete("mistral-large", prompt, session=session)

    st.chat_message(ASSISTANT).write(response)

    # st.chat_message(ASSISTANT).write(f"You wrote {prompt}")

# session = Session.builder.config("connection_name", "default").create()
# print(Complete("mistral-large", "how do snowflakes get their unique patterns?"))
