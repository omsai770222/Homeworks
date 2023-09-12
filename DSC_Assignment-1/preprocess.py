import sys
import pandas as pd
from flask import Flask, render_template

app = Flask(__name__)

def process_data(input_csv, output_csv):
    try:
        
        df = pd.read_csv(input_csv)

       
        df['high2'] = df['high'] * 2

        
        df.to_csv(output_csv, index=False)
        return f"Data processed and saved to {output_csv}"
    except Exception as e:
        return f"Error: {e}"

@app.route('/')
def index():
    input_csv = sys.argv[1]
    output_csv = sys.argv[2]
    result = process_data(input_csv, output_csv)
    return f"<h1>{result}</h1>"

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python data_processor.py input.csv output.csv")
    else:
        app.run(host='0.0.0.0', port=8888)
