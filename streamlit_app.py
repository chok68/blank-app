import yaml
import subprocess
import snowflake.snowpark
import streamlit as st
import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.cortex import Complete


def run_command(cmd):
    print('run_command: starts')
    print(f'   running command: "{cmd}"')
    subprocess.run(cmd.split())


def load_text_from_file(filename_txt):
    with open(filename_txt) as f:
        text = f.read()
    return text


def extract_text(pdffilename):
    pdftk_cmd = f"pdftotext {pdffilename} /tmp/pdftext.txt"
    run_command(pdftk_cmd)
    return load_text_from_file('/tmp/pdftext.txt')


def extract_fields(pdffilename):
    pdftk_cmd = f"pdftk {pdffilename} dump_data_fields output /tmp/pdffields.txt"
    run_command(pdftk_cmd)
    return load_text_from_file('/tmp/pdffields.txt')


session = Session.builder.config("connection_name", "default").create()

USER = "user"
ASSISTANT = "assistant"

prompt = None

# initialize prompting index
if "current_question_index" not in st.session_state:
  st.session_state["current_question_index"] = 0

pdf_filename = 'my_upload2.pdf'

def on_file_uploaded():
  print('on_file_uploaded: starts')
  my_upload = st.session_state['uploaded_pdf']
  b = my_upload.getvalue()
  print('   b: ', len(b))
  with open(pdf_filename, "wb") as f:
    f.write(b)
  st.session_state['has_to_load_pdf'] = True
  print('on_file_uploaded: ends')

st.file_uploader('I will help you fill your PDF form', type='pdf', key='uploaded_pdf', on_change=on_file_uploaded)


if 'has_to_load_pdf' in st.session_state and st.session_state['has_to_load_pdf']:
  st.chat_message(ASSISTANT).write(f'''Thanks for uploading your PDF file. Your file was 
                                  successfully saved.
                                  We can now proceed with questions...''')

  # User has uploaded the pdf file. Let's extract the text that might contain form filling instructions
  pdf_text = extract_text(pdf_filename)
  # print('-' * 40)
  # print(f'pdf_text: {pdf_text[:200]}')

  # We need the field names from the pdf
  pdf_fields = extract_fields(pdf_filename)
  # print('-' * 40)
  # print(f'pdf_fields: {pdf_fields[:200]}')

  #####
  # Build Wizard
  #####
  prompt = f'''
  You are assisting the user to fill pdf forms. 

  PDF forms usually contains instructions that the user is required to follow.

  Important: there might be important instructions at the bottom of the RAW TEXT
  related to fields present at the beginning of the RAW TEXT.

  Your job is to analyze the RAW TEXT and extract questions to guide the user that must be formatted in Yaml format.

  This is the raw text:
  --- RAW TEXT BEGINS ---
  {pdf_text}
  --- RAW TEXT ENDS ---

  The yaml response cannot include markdown (such as ```yaml ... ```), just the plain question object and must be formatted using this format:

  - question: the main question extracted from the provided text
    instructions: the extracted instructions from the provided text
    hints: related extracted from text that is far from the main question text
  '''
  # print(prompt)

  print('asking ai...')
  complete_response = Complete("mistral-large2", prompt, session=session)
  print('got ai response...')

  response = complete_response
  # response = fake_response

  # with st.chat_message("assistant"):
  #    st.markdown(response)

  response = response.replace('`', '').replace('yaml', '')
  questions = yaml.safe_load(response)
  print(questions)

  st.session_state['has_to_load_pdf'] = False

  st.session_state['has_to_answer'] = True
  st.session_state['questions'] = questions


if 'has_to_answer' in st.session_state and st.session_state['has_to_answer']:
  questions = st.session_state['questions']

  current_question_index = int(st.session_state["current_question_index"])
  print(f'current_question_index: {current_question_index}')
  question = questions[current_question_index]
  with st.chat_message("assistant"):
      markdown = f"**Question #{current_question_index + 1}: {question['question']}** \n {question['instructions']}"
      st.markdown(markdown)

  def on_question_answered():
    answer = st.session_state.answer
    print(f'answer: {answer}')
    if answer == 'begin':
      st.session_state["current_question_index"] = 0
    elif answer == 'prev':
      st.session_state["current_question_index"] = int(st.session_state["current_question_index"]) - 1
    else:
      st.session_state["current_question_index"] = int(st.session_state["current_question_index"]) + 1

  st.chat_input(f"", on_submit=on_question_answered, key='answer')
