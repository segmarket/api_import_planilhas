from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import joinedload
import re
from datetime import datetime
import requests

app = Flask(__name__)   
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://rwvtorlohmrtob:80345d4c65f0d50ffc13198838a5195412213a02a7dca8930c77b0860414d97c@ec2-44-211-104-233.compute-1.amazonaws.com/ddk2jvcro9q8ff'
db = SQLAlchemy(app)

ZM_API_URL = 'https://segcam.segmarket.com.br/zm/api'
ZM_USER = 'api'
ZM_PASSWORD = 'Mundo@108'

def get_auth_token():
    url = f"{ZM_API_URL}/host/login.json"
    response = requests.post(url, data={'user': ZM_USER, 'pass': ZM_PASSWORD})
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        return None

class Mercado(db.Model):
    __tablename__ = 'mercado'
    idmercado = db.Column(db.BigInteger, primary_key=True)

class Venda(db.Model):
    __tablename__ = 'vendas_import'
    id = db.Column(db.Integer, primary_key=True)
    idmercado = db.Column(db.BigInteger, db.ForeignKey('mercado.idmercado'))
    data_hora = db.Column(db.DateTime)
    valor = db.Column(db.Float)
    produto = db.Column(db.String())
    quantidade = db.Column(db.Integer, nullable=True)
    idvenda_exter = db.Column(db.String())  # Nova coluna para armazenar UUID ou número
    mercado = db.relationship('Mercado', backref=db.backref('vendas_import', lazy=True))

with app.app_context():
    db.create_all()

@app.route('/upload-vendas', methods=['POST'])
def upload_vendas():
    mercado_id = request.form['idmercado']
    layout = request.form['layout']
    file = request.files['file']
    
    if not file:
        return "Nenhum arquivo enviado.", 400

    skiprows = 0 if layout == 'amlabs' else 14
    df = pd.read_excel(file.stream, skiprows=skiprows, header=0)

    if layout == 'amlabs':
        colunas = {
            'data_hora': 'Data/Hora',
            'valor': 'Valor',
            'produto': 'Descrição produto',
            'quantidade': 'Quantidade',
            'idvenda_exter': 'Cód. interno'  # Coluna para idvenda_exter
        }
    elif layout == 'vmpay':
        colunas = {
            'data_hora': 'Data/hora',
            'valor': 'Valor (R$)',
            'produto': 'Produto',
            'quantidade': 'Quantidade',
            'idvenda_exter': 'Requisição'  # Coluna para idvenda_exter
        }

    df[colunas['data_hora']] = pd.to_datetime(df[colunas['data_hora']])
    data_planilha = df[colunas['data_hora']].min().date()

    vendas_existentes = db.session.query(func.count(Venda.id)).filter(
        func.date(Venda.data_hora) == data_planilha,
        Venda.idmercado == mercado_id
    ).scalar()
    
    if vendas_existentes > 0:
        return jsonify({"erro": "Vendas para este mercado e data já foram importadas."}), 400

    for _, row in df.iterrows():
        data_hora = row[colunas['data_hora']].to_pydatetime()
        idvenda_exter = row[colunas['idvenda_exter']]  # Extrai o valor de idvenda_exter
         # Extrair e converter a quantidade para vmpay
        if layout == 'vmpay':
            quantidade_str = row[colunas['quantidade']]
            match = re.match(r'(\d+)', quantidade_str)
            quantidade = int(match.group(1)) if match else 1
        else:
            quantidade = row.get(colunas['quantidade'], 1)
        venda = Venda(
            idmercado=mercado_id,
            data_hora=data_hora,
            valor=row[colunas['valor']],
            produto=row[colunas['produto']],
            quantidade=quantidade,
            idvenda_exter=idvenda_exter  # Atribui o valor extraído a idvenda_exter
        )
        db.session.add(venda)

    db.session.commit()

    return jsonify({"sucesso": "Dados importados com sucesso."}), 200

@app.route('/buscar_vendas', methods=['GET'])
def buscar_vendas():
    # Obtendo os dados do corpo da requisição
    dados = request.get_json()
    idmercado = dados.get('idmercado')
    data_venda = dados.get('data_venda')
    
    # Convertendo a data de string para objeto datetime, se fornecida
    if data_venda:
        data_venda = datetime.strptime(data_venda, '%Y-%m-%d')
    
    # Construindo a consulta com os parâmetros fornecidos
    query = Venda.query.filter(Venda.idmercado == idmercado)
    if data_venda:
        query = query.filter(db.func.date(Venda.data_hora) == data_venda.date())
    
    # Adicionando ordenação pelo ID em ordem ascendente
    vendas = query.order_by(Venda.id.asc()).options(joinedload(Venda.mercado)).all()
    
    vendas_agrupadas = {}
    for venda in vendas:
        if venda.idvenda_exter not in vendas_agrupadas:
            vendas_agrupadas[venda.idvenda_exter] = []
        vendas_agrupadas[venda.idvenda_exter].append({
            'id': venda.id,
            'data_hora': venda.data_hora.isoformat(),
            'valor': venda.valor,
            'produto': venda.produto,
            'quantidade': venda.quantidade
        })

    return jsonify(vendas_agrupadas)
 
@app.route('/link_video_event', methods=['GET'])
def get_event_video_link():
    event_id = request.args.get('event_id')
    if not event_id:
        return jsonify({"erro": "ID do evento não fornecido"}), 400
    
    auth_token = get_auth_token()
    if not auth_token:
        return jsonify({"erro": "Não foi possível autenticar"}), 401
    
    # Obtendo detalhes do evento específico
    url = f"{ZM_API_URL}/events/{event_id}.json?token={auth_token}"
    response = requests.get(url)
    if response.status_code == 200:
        event_details = response.json()['event']['Event']
        monitor_id = event_details['MonitorId']
        start_time = event_details['StartTime']
        event_date = start_time.split(' ')[0]
        
        # Construindo o link do vídeo assumindo um padrão específico de caminho
        video_url = f"https://segcam.segmarket.com.br/videos/{monitor_id}/{event_date}/{event_id}/{event_id}-video.mp4"
        
        return jsonify({"link_video": video_url})
    else:
        return jsonify({"erro": "Erro ao acessar a API ou evento não encontrado"}), response.status_code

if __name__ == '__main__':
    app.run(debug=False)

