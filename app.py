from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import func, create_engine
import re
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://seg:Mundo108@segmarket.ddns.net/hmg_cameras_db'
db = SQLAlchemy(app)

class Mercado(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(), unique=True)

class Venda(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    mercado_id = db.Column(db.Integer, db.ForeignKey('mercado.id'))
    data_hora = db.Column(db.DateTime)
    valor = db.Column(db.Float)
    produto = db.Column(db.String())
    quantidade = db.Column(db.Integer, nullable=True)  # Adicionado, permitindo NULL se for opcional
    mercado = db.relationship('Mercado', backref=db.backref('vendas', lazy=True))

with app.app_context():
    db.create_all()

@app.route('/upload-vendas', methods=['POST'])
def upload_vendas():
    mercado_id = request.form['mercado_id']
    layout = request.form['layout']
    file = request.files['file']
    
    if not file:
        return "Nenhum arquivo enviado.", 400

    skiprows = 0 if layout == 'amlabs' else 14  # Supondo que 'amlabs' não precisa pular linhas
    df = pd.read_excel(file.stream, skiprows=skiprows, header=0)  # header=0 para usar a primeira linha como cabeçalho

    if layout == 'amlabs':
        colunas = {'data_hora': 'Data/Hora', 'valor': 'Valor', 'produto': 'Descrição produto', 'quantidade': 'Quantidade'}
    elif layout == 'vmpay':
        colunas = {'data_hora': 'Data/hora', 'valor': 'Valor (R$)', 'produto': 'Produto', 'quantidade': 'Quantidade'}

    df[colunas['data_hora']] = pd.to_datetime(df[colunas['data_hora']])
    data_planilha = df[colunas['data_hora']].min().date()

    # Verifica duplicidade
    vendas_existentes = db.session.query(func.count(Venda.id)).filter(
        func.date(Venda.data_hora) == data_planilha,
        Venda.mercado_id == mercado_id
    ).scalar()
    
    if vendas_existentes > 0:
        return jsonify({"erro": "Vendas para este mercado e data já foram importadas."}), 400

    for _, row in df.iterrows():
        # Converter data/hora para datetime
        data_hora = row[colunas['data_hora']].to_pydatetime()

        # Extrair e converter a quantidade para vmpay
        if layout == 'vmpay':
            quantidade_str = row[colunas['quantidade']]
            match = re.match(r'(\d+)', quantidade_str)
            quantidade = int(match.group(1)) if match else 1
        else:
            quantidade = row.get(colunas['quantidade'], 1)
        venda = Venda(
            mercado_id=mercado_id,
            data_hora=data_hora,
            valor=row[colunas['valor']],
            produto=row[colunas['produto']],
            quantidade=quantidade
        )
        db.session.add(venda)

    db.session.commit()

    return jsonify({"sucesso": "Dados importados com sucesso."}), 200
    
if __name__ == '__main__':
    app.run(debug=True)
