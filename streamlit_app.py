import os
import yaml
import subprocess
import snowflake.snowpark
import streamlit as st
import snowflake.connector
import pandas as pd
from fdfgen import forge_fdf
from snowflake.snowpark.session import Session
from snowflake.cortex import Complete

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


def on_question_answered():
  answer = st.session_state.answer
  print(f'answer: {answer}')
  if answer == 'begin':
    st.session_state["current_question_index"] = 0
  elif answer == 'prev':
    st.session_state["current_question_index"] = int(st.session_state["current_question_index"]) - 1
  else:
    st.session_state["current_question_index"] = int(st.session_state["current_question_index"]) + 1
    st.session_state["answers"].append(answer)
    print('answers:', ' '.join(st.session_state["answers"]))


def on_narrative_submit():
  answer = st.session_state.answer
  print(f'answer: {answer}')
  st.session_state["user_narrative"] = answer


def translate_string_to_tuples(input_string):

    l = []
    
    for i, s in enumerate(input_string.split('\n')):

        if i == 0: continue

        fv = s.replace('"','').replace('Field: ','').split(',')
        if len(fv) >= 2:
            value = ','.join(fv[1:])
            value = value.strip()
            l.append((fv[0], value))

    return l

conn = st.connection('my_snowflake_connection', type='snowflake')
print('conn:', conn)
session = conn.session()

# session = Session.builder.config("connection_name", "my_connection").create()
# session = Session.builder.config("connection_name", "myconnection").create()
# print(f'session: {session}')
# session = Session.builder.config("connection_name", "default").create()

USER = "user"
ASSISTANT = "assistant"

prompt = None

# initialize prompting index
if "current_question_index" not in st.session_state:
  st.session_state["current_question_index"] = 0

st.file_uploader('I will help you fill your PDF form', type='pdf', key='uploaded_pdf', on_change=on_file_uploaded)

if 'has_to_load_pdf' in st.session_state and st.session_state['has_to_load_pdf']:
  st.chat_message(ASSISTANT).write(f'''Thanks for uploading your PDF file. Your file was 
                                  successfully saved and it is now being analyzed in 
                                  order to extract which form fields are to be filled.
                                  ''')

  # User has uploaded the pdf file. Let's extract the text that might contain form filling instructions
  pdf_text = extract_text(pdf_filename)
  st.session_state["pdf_text"] = pdf_text
  # print('-' * 40)
  # print(f'pdf_text: {pdf_text[:200]}')

  # We need the field names from the pdf
  pdf_fields = extract_fields(pdf_filename)
  st.session_state["pdf_fields"] = pdf_fields
  # print('-' * 40)
  # print(f'pdf_fields: {pdf_fields[:200]}')

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
  print(f'prompt:{prompt}')

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
  st.session_state['has_to_answer_wizard_type'] = True
  st.session_state['questions'] = questions
  st.session_state["answers"] = []


if 'has_to_answer_wizard_type' in st.session_state and st.session_state['has_to_answer_wizard_type']:
  option = st.selectbox(
      "How do you prefer to answer to the questions?",
      ("All answers at once using my own narrative", "One question at a time (coming soon!)"),
      key='answer_preference'
  )
  st.write("You selected:", option)


if 'answer_preference' in st.session_state and st.session_state['answer_preference'] == 'All answers at once using my own narrative':
  with st.chat_message("assistant"):
      markdown = f"""**You choose to answer based on your own narrative**"""
      st.markdown(markdown)
  text = st.chat_input(f"Paste your own narrative here", on_submit=on_narrative_submit, key='answer')


if 'user_narrative' in st.session_state and st.session_state['user_narrative']:
  pdf_text = st.session_state["pdf_text"]
  pdf_fields = st.session_state["pdf_fields"]

  user_narrative = st.session_state["user_narrative"]

  with st.chat_message("assistant"):
      markdown = f"""**I will try to fill your PDF form with your narrative**: \\
        {user_narrative}"""
      st.markdown(markdown)

  print(f'user_narrative: {user_narrative}')

  prompt = f"""
  You are assisting the user to fill form fields and based on free-form text called USER NARRATIVE that is passed below.

  The form has these instructions...

  ############### INSTRUCTIONS: START
  {pdf_text}
  ############### INSTRUCTIONS: END

  The form has multiple form fields defined inside FORM FIELDS. 

  ############### FORM FIELDS: START
  {pdf_fields}
  ############### FORM FIELDS: END

  Instructions for openai assistant:
  - Each field lives between --- blocks of characters
  - Analyse the input text and then find the best response to each field
  - If you don't find a valid response just return empty string instead of "Data not provided" or similar
  - Use the FieldNameAlt attribute to get the answer provided in the Answers provided by the user.
  - When you are asked to print output, use FieldName attribute instead of the FieldNameAlt attribute
  - The FieldType indicates if the field is Text, Button or other types.

  IMPORTANT: FieldName attribute must be printed and FieldNameAlt is ignored when you output your response.

  Response must be formatted in CSV format using FieldName and the best value you found.

  FieldName,Response

  ############### USER NARRATIVE: START
  {user_narrative}
  ############### USER NARRATIVE: END
  
  """

  print(f'prompt: {prompt}')

  print('asking ai...')
  complete_response = Complete("mistral-large2", prompt, session=session)
  print('got ai response...')

  response = complete_response
  # response = fake_response

  # fill form
  filled_fields = response

  # output directory
  output_dirname = '.'

  # Cleaning data
  filled_fields = filled_fields.replace('Data not provided', '')
  filled_fields = filled_fields.replace('DATA NOT PROVIDED', '')
  filled_fields = filled_fields.replace('Not provided', '')

  # fill fields by translating the string into a list of tuples
  fields = translate_string_to_tuples(filled_fields)

  # save filled fields
  output_filled_fields_filename = os.path.join(output_dirname, 'my-filled-fields.txt')
  os.unlink(output_filled_fields_filename)
  if not os.path.exists(output_filled_fields_filename):
      # Convert the list of tuples to a DataFrame
      df = pd.DataFrame(fields, columns=['FieldName','Response'])

      # Save the DataFrame to a CSV file
      df.to_csv(output_filled_fields_filename, index=False)

  else:
      # Load existing values
      df = pd.read_csv(output_filled_fields_filename)
      df.set_index('FieldName', inplace = True)
      print('df')
      print(df)

      # Convert the list of tuples to a DataFrame
      df2 = pd.DataFrame(fields, columns=['FieldName','Response'])
      df2.set_index('FieldName', inplace = True)
      print('df2')
      print(df2)

      # Modify in place using non-NA values from another DataFrame.
      print('updating df')
      df.update(df2)

      # Remove index not needed anymore
      df = df.reset_index()

      # Save the DataFrame to a CSV file
      df.to_csv(output_filled_fields_filename, index=False)

      # Convert DataFrame to a list of tuples
      fields = list(df.to_records(index=False))
      print('fields')
      print(fields)

  print('df')
  print(df)

  # save fields to fdf
  fdf = forge_fdf("",fields,[],[],[])

  # create a form definition fields to burn field values into pdf file
  with open("/tmp/data.fdf", "wb") as fdf_file:
      fdf_file.write(fdf)

  # save filled form
  output_pdf_filename = os.path.join(output_dirname, 'my-filled-form.pdf')

  print('-'*40)
  print('updating file: ', {output_pdf_filename})
  print('-'*40)
  pdftk_cmd = f"pdftk {pdf_filename} fill_form /tmp/data.fdf output {output_pdf_filename}"
  subprocess.run(pdftk_cmd.split())

  # download filled form
  with open(output_pdf_filename, 'rb') as f:
    st.download_button('Download your filled PDF form', f, file_name=output_pdf_filename, mime='application/pdf')  # Defaults to 'application/octet-stream'


if 'answer_preference' in st.session_state and st.session_state['answer_preference'] == 'One question at a time (coming soon!)':
  questions = st.session_state['questions']

  current_question_index = int(st.session_state["current_question_index"])
  print(f'current_question_index: {current_question_index}')

  question = questions[current_question_index]
  with st.chat_message("assistant"):
      markdown = f"""**Question #{current_question_index + 1}: {question['question']}** \\
        {question['instructions']}
      """
      st.markdown(markdown)

  st.chat_input(f"", on_submit=on_question_answered, key='answer')

