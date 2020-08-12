from flask import Flask,jsonify,request
from flask import render_template
import spacy
import ast

app = Flask(__name__)
labels = []
values = []
sentences=[]
print("Loading...")
MODELS = {
    "en_core_web_sm": spacy.load("en_core_web_sm")
}
print("Loaded!")

@app.route("/")
def get_chart_page():
    global labels,values
    labels = []
    values = []
    return render_template('chart.html', values=values, labels=labels)
@app.route('/refreshData')
def refresh_graph_data():
    global labels, values, sentences
    # print("labels now: " + str(labels))
    # print("data now: " + str(values))
    # print("sentences now: " + str(sentences))
    return jsonify(sLabel=labels, sData=values, sSentences=sentences)
@app.route('/updateData', methods=['POST'])
def update_data():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error",400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])
    # print("labels received: " + str(labels))
    # print("data received: " + str(values))
    return "success",201
@app.route('/updateSentence', methods=['POST'])
def update_text():
    global sentences
    # print(request.form['sentences'])
    sentences = ast.literal_eval(request.form['sentences'])
    return "success",201

@app.route("/ent", methods=['POST'])
def entitry():
    content = request.json
    text = content['text']
    model = content['model']
    nlp = MODELS[model]
    doc = nlp(text)
    res = [
        {"start": ent.start_char, "end": ent.end_char, "type": ent.label_}
        for ent in doc.ents
    ]
    return jsonify(res)




if __name__ == "__main__":
    app.run(host='localhost', port=5001)